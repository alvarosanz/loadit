import wx
import os
from loadit.misc import humansize


class NewBatchDialog(wx.Dialog):

    def __init__(self, parent):
        super().__init__(parent)
        self.SetTitle('New Batch')
        self.SetSize((640, 480))

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(-1, 8)

        # Name
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        field_sizer.Add(wx.StaticText(self, label='Name:'), 0, wx.RIGHT + wx.ALIGN_LEFT, 5)
        self._name = wx.TextCtrl(self)
        self._name.Bind(wx.EVT_TEXT, self.update)
        field_sizer.Add(self._name, 1, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.EXPAND, 15)
        sizer.Add(-1, 8)

        notebook = wx.Notebook(self)

        # Files
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        self.files = list()
        self._files = wx.ListCtrl(panel, wx.ID_ANY, style=wx.LC_REPORT)
        self._files.InsertColumn(0, 'File', width=500)
        self._files.InsertColumn(1, 'Size', width=80)
        self._files.Bind(wx.EVT_LIST_ITEM_RIGHT_CLICK, self.show_menu)
        panel_sizer.Add(self._files, 1, wx.ALL + wx.EXPAND, 5)
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.clear_button = wx.Button(panel, id=wx.ID_ANY, label='Clear')
        self.clear_button.Bind(wx.EVT_BUTTON, self.clear_all)
        field_sizer.Add(self.clear_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.add_button = wx.Button(panel, id=wx.ID_ANY, label='Add')
        self.add_button.Bind(wx.EVT_BUTTON, self.add_files)
        field_sizer.Add(self.add_button, 0, wx.LEFT + wx.EXPAND, 5)
        panel_sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_RIGHT, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Files')

        # Comment
        panel = wx.Panel(notebook, id=wx.ID_ANY)
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        self._comment = wx.TextCtrl(panel, style=wx.TE_MULTILINE)
        panel_sizer.Add(self._comment, 1, wx.ALL + wx.EXPAND, 5)
        panel.SetSizer(panel_sizer)
        notebook.AddPage(panel, 'Comment')

        notebook.ChangeSelection(0)
        sizer.Add(notebook, 1, wx.ALL + wx.EXPAND, 5)

        # Buttons
        field_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.cancel_button = wx.Button(self, id=wx.ID_CANCEL)
        self.cancel_button.SetDefault()
        field_sizer.Add(self.cancel_button, 0, wx.LEFT + wx.EXPAND, 5)
        self.ok_button = wx.Button(self, id=wx.ID_OK)
        field_sizer.Add(self.ok_button, 0, wx.LEFT + wx.EXPAND, 5)
        sizer.Add(field_sizer, 0, wx.ALL + wx.ALIGN_BOTTOM + wx.ALIGN_RIGHT, 15)

        self.SetSizer(sizer)
        self.update(None)

    @property
    def name(self):
        return self._name.Value

    @property
    def comment(self):
        return self._comment.Value

    def update(self, event):
        
        if self.files:
            self.clear_button.Enabled = True

            if self._name.Value:
                self.ok_button.Enabled = True
            else:
                self.ok_button.Enabled = False

        else:
            self.clear_button.Enabled = False
            self.ok_button.Enabled = False

    def add_files(self, event):

        with wx.FileDialog(self, 'Select files', style=wx.FD_OPEN + wx.FD_FILE_MUST_EXIST + wx.FD_MULTIPLE, wildcard='PCH files (*.pch)|*.pch') as dialog:
        
            if dialog.ShowModal() == wx.ID_OK:

                for file in dialog.GetPaths():

                    if file not in self.files:
                        self.files.append(file)
                        self._files.InsertItem(len(self.files) - 1, file)
                        self._files.SetItem(len(self.files) - 1, 1, humansize(os.path.getsize(file)))
        
                self.update(None)

    def remove_file(self, event):
        item = self._files.GetFirstSelected()

        while item != -1:
            self.files.remove(self._files.GetItemText(item, 0))
            self._files.DeleteItem(item)
            item = self._files.GetFirstSelected()

        self.update(None)

    def clear_all(self, event):

        with wx.MessageDialog(self, 'Are you sure?', 'Clear files', style=wx.YES_NO + wx.NO_DEFAULT) as dialog:

            if dialog.ShowModal() == wx.ID_YES:
                self.files = list()
                self._files.DeleteAllItems()
                self.update(None)

    def do_queries(self, event):
        
        for i, query_file in enumerate(self.files):
            self.root.statusbar.SetStatusText(f"Performing query '{os.path.basename(query_file)}' ({i + 1} of {len(self.files)})...")
            self._files.SetItem(i, 2, 'In progress ...')
            self.database.query_from_file(query_file)
            self._files.SetItem(i, 2, 'Done')

        self.root.statusbar.SetStatusText('Done!')

    def show_menu(self, event):

        if self._files.GetFirstSelected() != -1:
            popupmenu = wx.Menu()
            menu_item = popupmenu.Append(wx.ID_ANY, 'Remove')
            self._files.Bind(wx.EVT_MENU, self.remove_file, menu_item)
            self._files.PopupMenu(popupmenu, event.GetPoint())
