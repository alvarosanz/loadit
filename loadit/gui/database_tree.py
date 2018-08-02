import wx
import os
import wx.lib.agw.hypertreelist
from loadit.misc import humansize
from loadit.gui.new_batch_dialog import NewBatchDialog
from loadit.gui.database_info_dialog import DatabaseInfoDialog
from loadit.gui.table_info_dialog import TableInfoDialog
from loadit.gui.check_dialog import CheckDialog


class DatabaseTree(wx.lib.agw.hypertreelist.HyperTreeList):

    def __init__(self, parent, root, database):
        super().__init__(parent, wx.ID_ANY, size=(320, -1), style=wx.TR_HAS_BUTTONS + wx.TR_HIDE_ROOT + wx.TR_SINGLE +
                         wx.lib.agw.hypertreelist.TR_ELLIPSIZE_LONG_ITEMS + wx.lib.agw.hypertreelist.LIST_AUTOSIZE_CONTENT_OR_HEADER)
        self.database = database
        self.parent = parent
        self.root = root
        self.AddColumn('Item', width=250)
        self.AddColumn('Size', width=70, flag=wx.ALIGN_RIGHT)
        self.update()
        self.Bind(wx.EVT_TREE_ITEM_RIGHT_CLICK, self.show_menu)

    def show_menu(self, event):
        popupmenu = wx.Menu()
        read_only = not self.parent.is_local and self.database.read_only
        item = event.GetItem()

        if item is self.root_item:
            menu_item = popupmenu.Append(wx.ID_ANY, 'Show Info')
            self.Bind(wx.EVT_MENU, self.database_info, menu_item)
            menu_item = popupmenu.Append(wx.ID_ANY, 'Refresh')
            self.Bind(wx.EVT_MENU, self.refresh, menu_item)
            menu_item = popupmenu.Append(wx.ID_ANY, 'Check Integrity')
            self.Bind(wx.EVT_MENU, self.check, menu_item)
        elif (item.GetParent() is self.tables or
              item.GetParent() and item.GetParent().GetParent() is self.tables or
              item.GetParent().GetParent() and item.GetParent().GetParent().GetParent() is self.tables):
            menu_item = popupmenu.Append(wx.ID_ANY, 'Show Info')
            self.Bind(wx.EVT_MENU, self.table_info, menu_item)
        elif item is self.batches or item.GetParent() is self.batches:
            menu_item = popupmenu.Append(wx.ID_ANY, 'New Batch')
            self.Bind(wx.EVT_MENU, self.new_batch, menu_item)
            menu_item.Enable(not read_only)

            if item.GetParent() is self.batches:
                menu_item = popupmenu.Append(wx.ID_ANY, 'Restore')
                self.Bind(wx.EVT_MENU, self.restore, menu_item)
                menu_item.Enable(not read_only)

        elif item is self.attachments or item.GetParent() is self.attachments:
            menu_item = popupmenu.Append(wx.ID_ANY, 'Add')
            self.Bind(wx.EVT_MENU, self.add_attachment, menu_item)
            menu_item.Enable(not read_only)

            if item.GetParent() is self.attachments:
                menu_item = popupmenu.Append(wx.ID_ANY, 'Download')
                self.Bind(wx.EVT_MENU, self.download_attachment, menu_item)

                menu_item = popupmenu.Append(wx.ID_ANY, 'Remove')
                self.Bind(wx.EVT_MENU, self.remove_attachment, menu_item)
                menu_item.Enable(not read_only)

        self.PopupMenu(popupmenu, event.GetPoint())

    def update(self):

        try:
            self.DeleteChildren(self.root_item)
        except AttributeError:
            self.root_item = self.AddRoot(self.database.header.name)

        self.tables = self.AppendItem(self.root_item, f'Tables ({len(self.database.header.tables)})')

        for table in self.database.header.tables.values():
            item = self.AppendItem(self.tables, table['name'])
            self.SetItemText(item, humansize(self.database.header.get_size(table['name'])), 1)

            for field, _ in table['columns'][2:]:
                self.SetItemText(self.AppendItem(item, field), humansize(self.database.header.get_size(table['name'], field)), 1)

            if table['query_functions']:
                item = self.AppendItem(item, 'Others')

                for field in table['query_functions']:
                    self.AppendItem(item, field)

        self.batches = self.AppendItem(self.root_item, f'Batches ({len(self.database.header.batches)})')

        for batch in self.database.header.batches:
            self.SetItemText(self.AppendItem(self.batches, text=batch[0]), humansize(self.database.header.get_batch_size(batch[0])), 1)

        self.attachments = self.AppendItem(self.root_item, f'Attachments ({len(self.database.header.attachments)})')

        for attachment, (_, nbytes) in self.database.header.attachments.items():
            self.SetItemText(self.AppendItem(self.attachments, attachment), humansize(nbytes), 1)

        self.Expand(self.root_item)
        self.Expand(self.tables)
        self.Expand(self.batches)
        self.Expand(self.attachments)

    def refresh(self, event):

        if self.parent.is_local:
            self.parent.database = self.root.client.load_database(self.database.path)
        else:
            self.parent.database = self.root.client.load_remote_database(self.database.path)

        self.parent.update()

    def database_info(self, event):

        with DatabaseInfoDialog(self.root, self.database) as dialog:
            dialog.ShowModal()

    def table_info(self, event):
        item = self.GetSelection()

        while not item.GetParent() is self.tables:
            item = item.GetParent()

        with TableInfoDialog(self.root, self.database.header.tables[item.GetText()], self.database) as dialog:
            dialog.ShowModal()

    def check(self, event):

        with CheckDialog(self.root, self.database.check(print_to_screen=False)) as dialog:
            dialog.ShowModal()

    def new_batch(self, event):

        with NewBatchDialog(self.root) as dialog:
        
            if dialog.ShowModal() == wx.ID_OK:

                try:
                    self.database.new_batch(dialog.files, dialog.name, dialog.comment)
                    self.parent.update()
                except Exception as e:
                    self.root.statusbar.SetStatusText(str(e))

    def restore(self, event):
        batch = self.GetSelection().GetText()
        batches = [batch[0] for batch in self.database.header.batches]
        
        with wx.SingleChoiceDialog(self.root, 'Batch:', 'Restore Database', batches) as dialog:
            dialog.SetSelection(batches.index(batch))

            if dialog.ShowModal() == wx.ID_OK:
                batch = dialog.GetStringSelection()

                try:
                    self.database.restore(batch)
                    self.parent.update()
                except Exception as e:
                    self.root.statusbar.SetStatusText(str(e))

    def add_attachment(self, event):
        
        with wx.FileDialog(self.root, 'Add attachment', style=wx.FD_DEFAULT_STYLE) as dialog:
        
            if dialog.ShowModal() == wx.ID_OK:

                try:
                    self.database.add_attachment(dialog.GetPath())
                    self.update()
                except Exception as e:
                    self.root.statusbar.SetStatusText(str(e))

    def download_attachment(self, event):
        attachment = self.GetSelection().GetText()

        with wx.FileDialog(self.root, 'Download attachment', style=wx.FD_SAVE + wx.FD_OVERWRITE_PROMPT) as dialog:
            dialog.SetFilename(attachment)

            if dialog.ShowModal() == wx.ID_OK:

                try:
                    self.database.download_attachment(attachment, dialog.GetPath())
                    self.update()
                except Exception as e:
                    self.root.statusbar.SetStatusText(str(e))

    def remove_attachment(self, event):

        with wx.MessageDialog(self.root, 'Are you sure?', 'Remove attachment', style=wx.YES_NO + wx.NO_DEFAULT) as dialog:

            if dialog.ShowModal() == wx.ID_YES:

                try:
                    self.database.remove_attachment(self.GetSelection().GetText())
                    self.update()
                except Exception as e:
                    self.root.statusbar.SetStatusText(str(e))