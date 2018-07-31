import wx


class CheckDialog(wx.Dialog):

    def __init__(self, parent, msg):
        super().__init__(parent)
        self.SetTitle('Integrity Check')
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(wx.TextCtrl(self, value=msg, style=wx.TE_MULTILINE + wx.TE_READONLY), 1, wx.ALL + wx.EXPAND, 15)
        self.SetSizer(sizer)
