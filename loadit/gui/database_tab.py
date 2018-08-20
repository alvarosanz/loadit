import wx
from loadit.gui.database_tree import DatabaseTree
from loadit.gui.results_panel import ResultsPanel
from loadit.gui.query_panel import QueryPanel
from loadit.gui.multiple_query_panel import MultipleQueryPanel
from loadit.gui.database_log_panel import DatabaseLogPanel
from loadit.log import custom_logging
import logging


log = logging.getLogger()


class DatabaseTab(wx.Panel):
    
    def __init__(self, parent, root, database, is_local=True):
        super().__init__(parent=parent, id=wx.ID_ANY)
        self.root = root
        self.database = database
        self.is_local = is_local

        self.notebook = wx.Notebook(self)
        self.database_log_panel = DatabaseLogPanel(self.notebook, self.root)
        self.log = self.database_log_panel.log
        self.results_panel = ResultsPanel(self.notebook, self.root, self.log)
        self.single_query_panel = QueryPanel(self.notebook, self.root, self.database, self.results_panel, self.log)
        self.multiple_query_panel = MultipleQueryPanel(self.notebook, self.root, self.database, self.log)

        self.tree = DatabaseTree(self, self.root, self.database, self.log)
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(self.tree, 0, wx.EXPAND | wx.ALL, 5)

        self.notebook.AddPage(self.single_query_panel, 'Single Query')
        self.notebook.AddPage(self.multiple_query_panel, 'Multiple Query')
        self.notebook.AddPage(self.results_panel, 'Results')
        self.notebook.AddPage(self.database_log_panel, 'Log')
        self.notebook.ChangeSelection(0)
        self.notebook.Bind(wx.EVT_NOTEBOOK_PAGE_CHANGED, self.on_tab_changing)

        sizer.Add(self.notebook, 1, wx.EXPAND | wx.ALL, 5)
        self.SetSizer(sizer)
        self.initial_message()

    @custom_logging
    def initial_message(self):
        log.info('Database loaded')

    def update(self):
        self.tree.database = self.database
        self.tree.update()
        self.single_query_panel.database = self.database
        self.single_query_panel.update()
        self.multiple_query_panel.database = self.database
        self.multiple_query_panel.update()

    def on_tab_changing(self, event):
        
        if event.GetSelection() != 2:
            event.Skip()
        else:

            if self.results_panel.results:
                self.results_panel.Enabled = True
                event.Skip()
            else:
                self.results_panel.Enabled = False
