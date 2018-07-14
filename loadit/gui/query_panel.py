import wx
import wx.lib.agw.ultimatelistctrl


class QueryPanel(wx.Panel):

    def __init__(self, parent, root, database):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.database = database
        
        sizer = wx.BoxSizer(wx.VERTICAL)

        # Table
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Table:'), 0, wx.ALL + wx.ALIGN_RIGHT, 5)
        self._table = wx.Choice(self, choices=list(self.database.header.tables))
        field_sizer.Add(self._table, 1, wx.ALL + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 5)

        # Fields
        self.field_list = wx.lib.agw.ultimatelistctrl.UltimateListCtrl(self, wx.ID_ANY, agwStyle=wx.LC_REPORT | wx.LC_VRULES | wx.LC_HRULES | wx.LC_SINGLE_SEL)
        sizer.Add(self.field_list, 1, wx.ALL + wx.EXPAND, 5)
        
        self.SetSizer(sizer)