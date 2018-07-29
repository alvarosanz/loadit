import wx


class ConnectDialog(wx.Dialog):

    def __init__(self, parent, address=None):
        super().__init__(parent)
        self.SetTitle('Connect')
        self.SetSize((280, 200))
        self._address = wx.TextCtrl(self, value=address if address else '')
        self._user = wx.TextCtrl(self)
        self._password = wx.TextCtrl(self, style=wx.TE_PASSWORD)
        self.InitUI()

    def InitUI(self):
        sizer = wx.BoxSizer(wx.VERTICAL)

        field_sizer = wx.FlexGridSizer(3, 2, 10, 15)
        field_sizer.AddGrowableCol(1, 1)
        field_sizer.Add(wx.StaticText(self, label='Address:'), 0, wx.ALIGN_CENTER_VERTICAL + wx.ALIGN_RIGHT)
        field_sizer.Add(self._address, 1, wx.EXPAND)
        field_sizer.Add(wx.StaticText(self, label='User:'), 0, wx.ALIGN_CENTER_VERTICAL + wx.ALIGN_RIGHT)
        field_sizer.Add(self._user, 1, wx.EXPAND)
        field_sizer.Add(wx.StaticText(self, label='Password:', style=wx.TE_PASSWORD), 0, wx.ALIGN_CENTER_VERTICAL + wx.ALIGN_RIGHT)
        field_sizer.Add(self._password, 1, wx.EXPAND)

        sizer.Add(field_sizer, 1, wx.ALL + wx.EXPAND, 20)
        sizer.Add(self.CreateButtonSizer(wx.OK + wx.CANCEL), 0, wx.EXPAND + wx.BOTTOM + wx.TOP, 5)
        
        self.SetSizer(sizer)


    @property
    def address(self):
        return self._address.Value

    @property
    def user(self):
        return self._user.Value

    @property
    def password(self):
        return self._password.Value