package org.schabi.newpipe.local.subscription.dialog

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.wrapContentHeight
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.Checkbox
import androidx.compose.material3.CheckboxDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.ComposeView
import androidx.compose.ui.platform.ViewCompositionStrategy
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import androidx.fragment.app.DialogFragment
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.Completable
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.schedulers.Schedulers
import org.schabi.newpipe.R
import org.schabi.newpipe.database.feed.model.FeedGroupEntity
import org.schabi.newpipe.info_list.PipePipeComposeTheme
import org.schabi.newpipe.local.feed.FeedDatabaseManager
import org.schabi.newpipe.util.ThemeHelper

class FeedGroupSelectionDialog : DialogFragment() {

    private var subscriptionId: Long = 0
    private lateinit var feedDatabaseManager: FeedDatabaseManager
    private val disposables = CompositeDisposable()

    // Compose states
    private var allGroups by mutableStateOf<List<FeedGroupEntity>>(emptyList())
    private val selectedGroupIds = mutableStateListOf<Long>()
    private val originalGroupIds = mutableSetOf<Long>()
    private var isDataLoaded by mutableStateOf(false)

    companion object {
        private const val KEY_SUBSCRIPTION_ID = "subscription_id"

        @JvmStatic
        fun newInstance(subscriptionId: Long): FeedGroupSelectionDialog {
            val dialog = FeedGroupSelectionDialog()
            val args = Bundle().apply {
                putLong(KEY_SUBSCRIPTION_ID, subscriptionId)
            }
            dialog.arguments = args
            return dialog
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        subscriptionId = requireArguments().getLong(KEY_SUBSCRIPTION_ID)
        feedDatabaseManager = FeedDatabaseManager(requireContext())
        setStyle(STYLE_NO_TITLE, ThemeHelper.getMinWidthDialogTheme(requireContext()))
    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        loadDataSynchronously()

        return ComposeView(requireContext()).apply {
            setViewCompositionStrategy(ViewCompositionStrategy.DisposeOnViewTreeLifecycleDestroyed)
            setContent {
                PipePipeComposeTheme(context) {
                    FeedGroupSelectionContent(
                        allGroups = allGroups,
                        selectedGroupIds = selectedGroupIds,
                        isDataLoaded = isDataLoaded,
                        onToggleGroup = { groupId, checked ->
                            if (checked) {
                                if (!selectedGroupIds.contains(groupId)) {
                                    selectedGroupIds.add(groupId)
                                }
                            } else {
                                selectedGroupIds.remove(groupId)
                            }
                        },
                        onCancel = { dismiss() },
                        onSave = { saveChanges() }
                    )
                }
            }
        }
    }

    private fun loadDataSynchronously() {
        val loadDisposable = feedDatabaseManager.groups()
            .firstOrError()
            .flatMap { groups ->
                val membershipChecks = groups.map { group ->
                    feedDatabaseManager.subscriptionIdsForGroup(group.uid)
                        .firstOrError()
                        .map { subscriptionIds ->
                            GroupMembershipInfo(group.uid, subscriptionIds.contains(subscriptionId))
                        }
                }
                if (membershipChecks.isEmpty()) {
                    Single.just(Pair(groups, emptyList<GroupMembershipInfo>()))
                } else {
                    Single.zip(membershipChecks) { array ->
                        val list = array.map { it as GroupMembershipInfo }
                        Pair(groups, list)
                    }
                }
            }
            .subscribeOn(Schedulers.io())
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { (groups, membershipInfos) ->
                    allGroups = groups
                    selectedGroupIds.clear()
                    originalGroupIds.clear()
                    for (info in membershipInfos) {
                        if (info.isMember) {
                            selectedGroupIds.add(info.groupId)
                            originalGroupIds.add(info.groupId)
                        }
                    }
                    isDataLoaded = true
                },
                { throwable ->
                    isDataLoaded = true
                }
            )
        disposables.add(loadDisposable)
    }

    private fun saveChanges() {
        if (!isDataLoaded) {
            return
        }

        val toAdd = selectedGroupIds.toSet() - originalGroupIds
        val toRemove = originalGroupIds - selectedGroupIds.toSet()

        val completables = mutableListOf<Completable>()

        for (groupId in toAdd) {
            val addOp = feedDatabaseManager.subscriptionIdsForGroup(groupId)
                .firstOrError()
                .flatMapCompletable { currentIds ->
                    val newIds = currentIds.toMutableList()
                    if (!newIds.contains(subscriptionId)) {
                        newIds.add(subscriptionId)
                    }
                    feedDatabaseManager.updateSubscriptionsForGroup(groupId, newIds)
                }
            completables.add(addOp)
        }

        for (groupId in toRemove) {
            val removeOp = feedDatabaseManager.subscriptionIdsForGroup(groupId)
                .firstOrError()
                .flatMapCompletable { currentIds ->
                    val newIds = currentIds.toMutableList()
                    newIds.remove(subscriptionId)
                    feedDatabaseManager.updateSubscriptionsForGroup(groupId, newIds)
                }
            completables.add(removeOp)
        }

        if (completables.isEmpty()) {
            dismiss()
            return
        }

        val saveDisposable = Completable.merge(completables)
            .subscribeOn(Schedulers.io())
            .observeOn(AndroidSchedulers.mainThread())
            .subscribe(
                { dismiss() },
                { dismiss() }
            )
        disposables.add(saveDisposable)
    }

    override fun onDestroy() {
        super.onDestroy()
        disposables.clear()
    }

    private class GroupMembershipInfo(val groupId: Long, val isMember: Boolean)
}

@Composable
fun FeedGroupSelectionContent(
    allGroups: List<FeedGroupEntity>,
    selectedGroupIds: List<Long>,
    isDataLoaded: Boolean,
    onToggleGroup: (Long, Boolean) -> Unit,
    onCancel: () -> Unit,
    onSave: () -> Unit
) {
    Surface(
        modifier = Modifier
            .fillMaxWidth()
            .wrapContentHeight(),
        shape = MaterialTheme.shapes.extraLarge,
        color = MaterialTheme.colorScheme.surface,
        tonalElevation = 6.dp
    ) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(24.dp)
        ) {
            Text(
                text = stringResource(R.string.select_groups),
                style = MaterialTheme.typography.titleLarge,
                color = MaterialTheme.colorScheme.onSurface,
                modifier = Modifier.padding(bottom = 16.dp)
            )

            Box(
                modifier = Modifier
                    .fillMaxWidth()
                    .heightIn(max = 280.dp),
                contentAlignment = Alignment.Center
            ) {
                if (!isDataLoaded) {
                    CircularProgressIndicator(
                        color = MaterialTheme.colorScheme.primary,
                        modifier = Modifier.padding(vertical = 32.dp)
                    )
                } else if (allGroups.isEmpty()) {
                    Text(
                        text = "No groups available",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.padding(vertical = 32.dp)
                    )
                } else {
                    LazyColumn(
                        modifier = Modifier.fillMaxWidth(),
                        verticalArrangement = Arrangement.spacedBy(4.dp)
                    ) {
                        items(allGroups) { group ->
                            val isChecked = selectedGroupIds.contains(group.uid)
                            Row(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .clip(MaterialTheme.shapes.medium)
                                    .clickable { onToggleGroup(group.uid, !isChecked) }
                                    .padding(vertical = 8.dp, horizontal = 12.dp),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Checkbox(
                                    checked = isChecked,
                                    onCheckedChange = { onToggleGroup(group.uid, it) },
                                    colors = CheckboxDefaults.colors(
                                        checkedColor = MaterialTheme.colorScheme.primary,
                                        uncheckedColor = MaterialTheme.colorScheme.outline
                                    )
                                )
                                Spacer(modifier = Modifier.width(12.dp))
                                Text(
                                    text = group.name,
                                    style = MaterialTheme.typography.bodyLarge,
                                    color = MaterialTheme.colorScheme.onSurface
                                )
                            }
                        }
                    }
                }
            }

            Spacer(modifier = Modifier.height(24.dp))

            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.End,
                verticalAlignment = Alignment.CenterVertically
            ) {
                TextButton(
                    onClick = onCancel,
                    modifier = Modifier.padding(end = 8.dp)
                ) {
                    Text(text = stringResource(android.R.string.cancel))
                }

                Button(
                    onClick = onSave,
                    enabled = isDataLoaded
                ) {
                    Text(text = stringResource(android.R.string.ok))
                }
            }
        }
    }
}
