import wx
import os
from loadit.misc import humansize
from loadit.gui.table_info_dialog import TableInfoDialog


class DatabaseInfoDialog(wx.Dialog):

    def __init__(self, parent, database, active_tab=0):
        super().__init__(parent)
        self.database = database
        self.SetTitle('Database Info')
        self.SetSize((640, 480))

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        # General info
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Name:', size=(50, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        field_sizer.Add(wx.TextCtrl(self, value=os.path.basename(database.path), style=wx.TE_READONLY), 1, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.TOP + wx.EXPAND, 15)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Size:', size=(50, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        field_sizer.Add(wx.TextCtrl(self, value=humansize(database.header.nbytes), style=wx.TE_READONLY), 1, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.TOP + wx.EXPAND, 15)

        if database.header.batches:
            field_sizer = wx.BoxSizer(wx.HORIZONTAL)
            field_sizer.Add(wx.StaticText(self, label='Date:', size=(50, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
            field_sizer.Add(wx.TextCtrl(self, value=database.header.batches[-1][2], style=wx.TE_READONLY), 1, wx.LEFT + wx.EXPAND, 5)
            sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.TOP + wx.EXPAND, 15)
            field_sizer = wx.BoxSizer(wx.HORIZONTAL)
            field_sizer.Add(wx.StaticText(self, label='Hash:', size=(50, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
            field_sizer.Add(wx.TextCtrl(self, value=database.header.batches[-1][1], style=wx.TE_READONLY), 1, wx.LEFT + wx.EXPAND, 5)
            sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.TOP + wx.EXPAND, 15)

        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Version:', size=(50, -1)), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        field_sizer.Add(wx.TextCtrl(self, value=database.header.version, style=wx.TE_READONLY), 1, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.LEFT + wx.RIGHT + wx.TOP + wx.EXPAND, 15)

        sizer.Add(-1, 8)

        notebook = wx.Notebook(self)

        # Tables
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        self.tables = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT + wx.LC_SINGLE_SEL)
        self.tables.Bind(wx.EVT_LIST_ITEM_RIGHT_CLICK, self.show_menu)
        self.tables.InsertColumn(0, 'Table', width=250)
        self.tables.InsertColumn(1, 'Size', width=100)
        self.tables.InsertColumn(2, 'Fields', width=50)
        self.tables.InsertColumn(3, 'IDs', width=80)
        self.tables.InsertColumn(4, 'LIDs', width=80)

        for i, table in enumerate(database.header.tables.values()):
            self.tables.InsertItem(i, table['name'])
            self.tables.SetItem(i, 1, humansize(database.header.get_size(table['name'])))
            self.tables.SetItem(i, 2, str(len(table['columns'][2:])))
            self.tables.SetItem(i, 3, str(len(table['IDs'])))
            self.tables.SetItem(i, 4, str(len(table['LIDs'])))

        panel_sizer.Add(self.tables, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Tables ({})'.format(len(database.header.tables)))

        # Batches
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        self.batches = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT + wx.LC_SINGLE_SEL)
        self.batches.InsertColumn(0, 'Batch', width=250)
        self.batches.InsertColumn(1, 'Size', width=100)
        self.batches.InsertColumn(2, 'Date', width=170)
        self.batches.InsertColumn(3, 'Hash', width=80)
        self.batches.InsertColumn(4, 'Comment', width=400)

        for i, (batch, batch_hash, date, _, comment) in enumerate(database.header.batches):
            self.batches.InsertItem(i, batch)
            self.batches.SetItem(i, 1, humansize(database.header.get_batch_size(batch)))
            self.batches.SetItem(i, 2, date)
            self.batches.SetItem(i, 3, batch_hash)
            self.batches.SetItem(i, 4, comment)

        panel_sizer.Add(self.batches, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Batches ({})'.format(len(database.header.batches)))

        # Attachments
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        self.attachments = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT + wx.LC_SINGLE_SEL)
        self.attachments.InsertColumn(0, 'Attachment', width=250)
        self.attachments.InsertColumn(1, 'Size', width=100)

        for i, (attachment, (_, nbytes)) in enumerate(database.header.attachments.items()):
            self.attachments.InsertItem(i, attachment)
            self.attachments.SetItem(i, 1, humansize(nbytes))

        panel_sizer.Add(self.attachments, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Attachments ({})'.format(len(database.header.attachments)))

        notebook.ChangeSelection(active_tab)
        sizer.Add(notebook, 1, wx.ALL + wx.EXPAND, 5)

        self.SetSizer(sizer)

    def show_menu(self, event):
        popupmenu = wx.Menu()
        menu_item = popupmenu.Append(wx.ID_ANY, 'Show Info')
        self.tables.Bind(wx.EVT_MENU, self.table_info, menu_item)
        self.tables.PopupMenu(popupmenu, event.GetPoint())

    def table_info(self, event):

        with TableInfoDialog(self, self.database.header.tables[self.tables.GetItemText(self.tables.GetFocusedItem())], self.database) as dialog:
            dialog.ShowModal()