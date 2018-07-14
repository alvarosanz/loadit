import wx
from loadit.gui.database_tree import DatabaseTree
from loadit.gui.query_panel import QueryPanel


class DatabaseTab(wx.Panel):

    def __init__(self, parent, root, database, is_local=True):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.database = database
        self.is_local = is_local

        self.tree = DatabaseTree(self, self.root, self.database)
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(self.tree, 1, wx.EXPAND | wx.ALL, 10)

        notebook = wx.Notebook(self)

        self.inspector_panel = wx.Panel(notebook, wx.ID_ANY)
        notebook.AddPage(self.inspector_panel, 'Inspector')
        self.query_panel = QueryPanel(notebook, self.root, self.database)
        notebook.AddPage(self.query_panel, 'Query')
        notebook.ChangeSelection(0)

        sizer.Add(notebook, 2, wx.EXPAND | wx.ALL, 10)
        self.SetSizer(sizer)