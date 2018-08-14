import wx
import os
from loadit.misc import humansize


class TableInfoDialog(wx.Dialog):

    def __init__(self, parent, table, database):
        super().__init__(parent)
        self.SetTitle('Table Info')
        self.SetSize((640, 480))

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        # Name
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Name:'), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        field_sizer.Add(wx.TextCtrl(self, value=table['name'], style=wx.TE_READONLY), 1, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 15)

        notebook = wx.Notebook(self)

        # Fields
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        fields = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT)
        fields.InsertColumn(0, 'Field', width=100)
        fields.InsertColumn(1, 'Type', width=40)
        fields.InsertColumn(2, 'Size', width=80)

        for i, (field_name, dtype) in enumerate(table['columns'][2:]):
            fields.InsertItem(i, field_name)
            fields.SetItem(i, 1, dtype)
            fields.SetItem(i, 2, humansize(database.header.get_size(table['name'], field_name)))

        panel_sizer.Add(fields, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Fields ({})'.format(len(table['columns'][2:])))

        # IDs
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        panel_sizer.Add(wx.TextCtrl(panel, value=', '.join([str(x) for x in table['IDs']]), style=wx.TE_MULTILINE + wx.TE_READONLY, size=(400, 100)), 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, '{}s ({})'.format(table['columns'][1][0], len(table['IDs'])))

        # LIDs
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        panel_sizer.Add(wx.TextCtrl(panel, value=', '.join([str(x) for x in table['LIDs']]), style=wx.TE_MULTILINE + wx.TE_READONLY, size=(400, 100)), 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, '{}s ({})'.format(table['columns'][0][0], len(table['LIDs'])))

        notebook.ChangeSelection(0)
        sizer.Add(notebook, 1, wx.ALL + wx.EXPAND, 5)

        self.SetSizer(sizer)
