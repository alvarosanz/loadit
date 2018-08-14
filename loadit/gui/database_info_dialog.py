import wx
import os
from loadit.misc import humansize


class DatabaseInfoDialog(wx.Dialog):

    def __init__(self, parent, database, active_tab=0):
        super().__init__(parent)
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
        fields = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT)
        fields.InsertColumn(0, 'Table', width=250)
        fields.InsertColumn(1, 'Size', width=100)
        fields.InsertColumn(2, 'Fields', width=50)
        fields.InsertColumn(3, 'IDs', width=80)
        fields.InsertColumn(4, 'LIDs', width=80)

        for i, table in enumerate(database.header.tables.values()):
            fields.InsertItem(i, table['name'])
            fields.SetItem(i, 1, humansize(database.header.get_size(table['name'])))
            fields.SetItem(i, 2, str(len(table['columns'][2:])))
            fields.SetItem(i, 3, str(len(table['IDs'])))
            fields.SetItem(i, 4, str(len(table['LIDs'])))

        panel_sizer.Add(fields, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Tables ({})'.format(len(database.header.tables)))

        # Batches
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        fields = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT)
        fields.InsertColumn(0, 'Batch', width=250)
        fields.InsertColumn(1, 'Size', width=100)
        fields.InsertColumn(2, 'Date', width=170)
        fields.InsertColumn(3, 'Hash', width=80)
        fields.InsertColumn(4, 'Comment', width=400)

        for i, (batch, batch_hash, date, _, comment) in enumerate(database.header.batches):
            fields.InsertItem(i, batch)
            fields.SetItem(i, 1, humansize(database.header.get_batch_size(batch)))
            fields.SetItem(i, 2, date)
            fields.SetItem(i, 3, batch_hash)
            fields.SetItem(i, 4, comment)

        panel_sizer.Add(fields, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Batches ({})'.format(len(database.header.batches)))

        # Attachments
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        fields = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT)
        fields.InsertColumn(0, 'Attachment', width=250)
        fields.InsertColumn(1, 'Size', width=100)

        for i, (attachment, (_, nbytes)) in enumerate(database.header.attachments.items()):
            fields.InsertItem(i, attachment)
            fields.SetItem(i, 1, humansize(nbytes))

        panel_sizer.Add(fields, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Attachments ({})'.format(len(database.header.attachments)))

        notebook.ChangeSelection(active_tab)
        sizer.Add(notebook, 1, wx.ALL + wx.EXPAND, 5)

        self.SetSizer(sizer)
