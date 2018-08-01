import wx
from loadit.gui.database_tree import DatabaseTree
from loadit.gui.query_panel import QueryPanel
from loadit.gui.multiple_query_panel import MultipleQueryPanel
from loadit.gui.results_panel import ResultsPanel


class DatabaseTab(wx.Panel):

    def __init__(self, parent, root, database, is_local=True):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.database = database
        self.is_local = is_local

        self.tree = DatabaseTree(self, self.root, self.database)
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(self.tree, 0, wx.EXPAND | wx.ALL, 5)

        notebook = wx.Notebook(self)

        self.results_panel = ResultsPanel(notebook, self.root)
        notebook.AddPage(self.results_panel, 'Results')
        self.single_query_panel = QueryPanel(notebook, self.root, self.database, self.results_panel)
        notebook.AddPage(self.single_query_panel, 'Single Query')
        self.multiple_query_panel = MultipleQueryPanel(notebook, self.root, self.database)
        notebook.AddPage(self.multiple_query_panel, 'Multiple Query')
        notebook.ChangeSelection(1)
        notebook.Bind(wx.EVT_NOTEBOOK_PAGE_CHANGED, self.on_tab_changing)

        sizer.Add(notebook, 1, wx.EXPAND | wx.ALL, 5)
        self.SetSizer(sizer)

    def update(self):
        self.single_query_panel.update()
        self.multiple_query_panel.update()

    def on_tab_changing(self, event):
        
        if event.GetSelection() != 0 or self.results_panel.results:
            event.Skip()
