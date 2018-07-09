import os
import sys
import threading
from tkinter import filedialog
from tkinter import simpledialog
from tkinter import messagebox
from tkinter import *
from tkinter import ttk
from loadit.client import Client
from loadit.misc import humansize


class App(object):

    def __init__(self):
        self.client = Client()
        self.root = Tk()
        hsize_min, vsize_min = 1024, 640
        self.root.geometry(f'{hsize_min}x{vsize_min}')
        self.root.minsize(hsize_min, vsize_min)
        self.root.title('loadit')

        # Menu bar
        self.menu = Menu(self.root)
        self.root.config(menu=self.menu)

        # File menu
        self.file_menu = Menu(self.menu, postcommand=self.update_file_menu)
        self.file_menu.add_command(label='New...', command=self.create_local_database)
        self.file_menu.add_command(label='Open...', command=self.load_local_database)
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Close', command=self.close_database)
        self.file_menu.add_command(label='Exit', command=self.quit)
        self.menu.add_cascade(label='File', menu=self.file_menu)

        # Connection menu
        self.connection_menu = Menu(self.menu, postcommand=self.update_connection_menu)
        self.connection_menu.add_command(label='Connect...', command=self.connect)
        self.connection_menu.add_command(label='Disconnect', command=self.disconnect)
        self.connection_menu.add_separator()
        self.connection_menu.add_command(label='New...', command=self.create_remote_database)
        self.connection_menu.add_command(label='Open...', command=self.load_remote_database)
        self.connection_menu.add_separator()
        self.menu.add_cascade(label='Connection', menu=self.connection_menu)

        # Connection management menu
        self.management_menu = Menu(self.connection_menu)
        self.management_menu.add_command(label='Info...', command=self.connection_info)
        self.management_menu.add_command(label='Sessions...', command=self.sessions)
        self.management_menu.add_command(label='Sync Databases...', command=self.sync_databases)
        self.management_menu.add_command(label='Shutdown', command=self.shutdown)
        self.connection_menu.add_cascade(label='Management', menu=self.management_menu)

        # Main area
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.grid(row=0, sticky=W+E+N+S)
        self.notebook = ttk.Notebook(self.main_frame, padding=0)
        self.notebook.pack(fill='both', expand=True)
        self.remote_tabs = list()

        # Status Bar
        self.statusbar = StatusBar(self.root, self.client)
        self.statusbar.grid(row=1, sticky=W+E+N+S)
        sys.stdout = self.statusbar # Redirect stdout to statusbar

        # Configure grid
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        # Preload heavy module
        def import_heavy_modules():
            import loadit.queries

        threading.Thread(target=import_heavy_modules).start()

        self.root.mainloop()

    def update_file_menu(self):

        if self.notebook.tabs():
            self.file_menu.entryconfig('Close', state='normal')
        else:
            self.file_menu.entryconfig('Close', state='disabled')

    def update_connection_menu(self):
        self.connection_menu.entryconfig('Connect...', state='normal')
        self.connection_menu.entryconfig('Disconnect', state='disabled')
        self.connection_menu.entryconfig('New...', state='disabled')
        self.connection_menu.entryconfig('Open...', state='disabled')
        self.connection_menu.entryconfig('Management', state='disabled')

        if self.client._authentication:
            session = self.client.session
            self.connection_menu.entryconfig('Connect...', state='disabled')
            self.connection_menu.entryconfig('Disconnect', state='normal')

            if session['create_allowed']:
                self.connection_menu.entryconfig('New...', state='normal')

            self.connection_menu.entryconfig('Open...', state='normal')

            if session['is_admin']:
                self.connection_menu.entryconfig('Management', state='normal')

        self.connection_menu.config(postcommand=self.update_connection_menu)

    def close_database(self):
        self.notebook.forget(self.notebook.index('current'))
        self.statusbar.refresh('Database closed')

    def quit(self):
        self.root.destroy()

    def create_local_database(self):

        try:
            folder = filedialog.askdirectory(parent=self.root)

            if folder:
                name = simpledialog.askstring('New database', 'Name:', parent=self.root)

                if name:
                    database = os.path.join(folder, name)
                    self.notebook.add(DatabaseFrame(self.client.create_database(database), self.root, self.statusbar),
                                      text=os.path.basename(database))
                    self.notebook.select(self.notebook.tabs()[-1])

        except Exception as e:
            self.statusbar.refresh(e)

    def create_remote_database(self):

        try:
            database = simpledialog.askstring('New database', 'Database:', parent=self.root)

            if name:
                self.notebook.add(DatabaseFrame(self.client.create_remote_database(database), self.root, self.statusbar, is_local=False),
                                  text=os.path.basename(database))
                self.remote_tabs.append(self.notebook.tabs()[-1])
                self.notebook.select(self.remote_tabs[-1])

        except Exception as e:
            self.statusbar.refresh(e)

    def load_local_database(self):

        try:
            database = filedialog.askdirectory(parent=self.root)

            if database:
                self.notebook.add(DatabaseFrame(self.client.load_database(database), self.root, self.statusbar),
                                  text=os.path.basename(database))
                self.notebook.select(self.notebook.tabs()[-1])
                self.statusbar.refresh('Database loaded')

        except Exception as e:
            self.statusbar.refresh(e)

    def load_remote_database(self):

        try:
            dialog = LoadRemoteDatabaseDialog(self.root, sorted(self.client.remote_databases), 'Open database')

            if dialog.data:
                database = dialog.data
                self.notebook.add(DatabaseFrame(self.client.load_remote_database(database), self.root, self.statusbar, is_local=False),
                                  text=os.path.basename(database))
                self.remote_tabs.append(self.notebook.tabs()[-1])
                self.notebook.select(self.remote_tabs[-1])
                self.statusbar.refresh('Database loaded')

        except Exception as e:
            self.statusbar.refresh(e)

    def connect(self):

        try:
            dialog = ConnectDialog(self.root, 'Connect')

            if dialog.data:
                self.statusbar.refresh('Connecting...')
                server_address, user, password = dialog.data
                self.client.connect(server_address, user, password)
                self.statusbar.refresh()

        except Exception as e:
            self.statusbar.refresh(e)

    def disconnect(self):
        self.client._authentication = None

        for tab_id in self.remote_tabs:
            self.notebook.forget(tab_id)

        self.remote_tabs = list()
        self.statusbar.refresh('Logged out')

    def connection_info(self):
        pass

    def sessions(self):
        pass

    def sync_databases(self):
        pass

    def shutdown(self):
        pass


class StatusBar(ttk.Frame):

    def __init__(self, parent, client):
        super().__init__(parent)
        self.client = client
        self.status = StringVar()
        self.connection_status = StringVar()

        self.label = ttk.Label(self, textvariable=self.status, padding=2)
        self.label.grid(row=0, column=0, sticky=W+E)

        self.connection_label = ttk.Label(self, textvariable=self.connection_status, padding=2, anchor=E)
        self.connection_label.grid(row=0, column=1, sticky=W+E)

        self.grid_columnconfigure(0, weight=1)
        self._update_connection_status()

    def refresh(self, msg=None):

        if not msg is None:
            self.status.set(msg)

        self._update_connection_status()
        self.label.update()
        self.connection_label.update()

    def _update_connection_status(self):

        if self.client._authentication:
            self.connection_status.set(self.client.session['user'])
        else:
            self.connection_status.set('Not connected')

    def write(self, msg):

        if msg != '\n':
            self.status.set(msg.replace('\n', '; '))
            self.label.update()


class DatabaseFrame(ttk.Frame):

    def __init__(self, database, root, statusbar, is_local=True):
        self.database = database
        self.root = root
        self.statusbar = statusbar
        self.is_local = is_local

        super().__init__()
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, minsize=400)
        self.grid_columnconfigure(1, weight=1)

        # Treeview
        tree_frame = ttk.Frame(self)
        tree_frame.grid(row=0, column=0, sticky=W+E+N+S, padx=5, pady=5)
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        self.tree = ttk.Treeview(tree_frame, selectmode='browse', columns=['Size'])
        self.tree.grid(row=0, column=0, sticky=W+E+N+S)
        self.tree.column('Size', stretch=False, width=90, minwidth=90, anchor='e')
        self.tree.heading('Size', text='Size', anchor='w')
        self.tree.bind('<Button-2>', self.show_tree_menu)
        self.tree.bind('<Button-3>', self.show_tree_menu)
        self.update_tree()

        # Tree scrollbars
        vsb = ttk.Scrollbar(tree_frame, orient='vertical', command=self.tree.yview)
        vsb.grid(row=0, column=1, sticky=W+E+N+S)

        hsb = ttk.Scrollbar(tree_frame, orient='horizontal', command=self.tree.xview)
        hsb.grid(row=1, column=0, sticky=W+E+N+S)

        self.tree.configure(xscrollcommand=hsb.set, yscrollcommand=vsb.set)

        # Main pane
        main_frame = ttk.Frame(self)
        main_frame.grid(row=0, column=1, sticky=W+E+N+S, padx=5, pady=5)
        main_frame.grid_rowconfigure(0, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        notebook = ttk.Notebook(main_frame, padding=5)
        notebook.pack(fill='both', expand=True)
        inspector_frame = ttk.Frame()
        notebook.add(inspector_frame, text='Inspector')
        query_frame = ttk.Frame()
        notebook.add(query_frame, text='Query')

    def update_tree(self):

        for item in ('tables', 'batches', 'attachments'):

            if self.tree.exists(item):
                self.tree.delete(item)

        self.tree.insert('', 'end', 'tables', text='Tables', open=True, tags=['tables'])
        self.tree.insert('', 'end', 'batches', text='Batches', open=True, tags=['batches'])
        self.tree.insert('', 'end', 'attachments', text='Attachments', open=True, tags=['attachments'])

        for table in self.database.header.tables.values():
            self.tree.insert('tables', 'end', 'tables::' + table['name'], text=table['name'])
            self.tree.set('tables::' + table['name'], 'Size', humansize(table['nbytes']))

            for field, _ in table['columns'][2:]:
                self.tree.insert('tables::' + table['name'], 'end', 'tables::' + table['name'] + '::' + field, text=field)
                self.tree.set('tables::' + table['name'] + '::' + field, 'Size', humansize(self.database.header.get_size(table['name'], field)))

            if table['query_functions']:
                self.tree.insert('tables::' + table['name'], 'end', 'tables::' + table['name'] + '::others', text='Others')

                for field in table['query_functions']:
                    self.tree.insert('tables::' + table['name'] + '::others', 'end', 'tables::' + table['name'] + '::others::' + field, text=field)

        for batch in self.database.header.batches:
            self.tree.insert('batches', 'end', 'batches::' + batch[0], text=batch[0],
                             tags=['batches', 'batches_children'])
            self.tree.set('batches::' + batch[0], 'Size', humansize(self.database.header.get_batch_size(batch[0])))

        for attachment, (_, nbytes) in self.database.header.attachments.items():
            self.tree.insert('attachments', 'end', 'attachments::' + attachment, text=attachment,
                             tags=['attachments', 'attachments_children'])
            self.tree.set('attachments::' + attachment, 'Size', humansize(nbytes))

    def show_tree_menu(self, event):
        item = self.tree.identify('item', event.x, event.y)

        if item:
            self.tree.selection_set(item)
            self.tree.focus_set()
            self.tree.focus(item)
            menu = Menu(self.root, tearoff=0)
            read_only = not self.is_local and self.database.read_only

            if self.tree.tag_has('batches', item):

                if read_only:
                    menu.add_command(label='New', command=self.new_batch, state='disabled')
                else:
                    menu.add_command(label='New', command=self.new_batch, state='normal')

            if self.tree.tag_has('batches_children', item):
                menu.add_separator()

                if read_only:
                    menu.add_command(label='Restore', command=self.restore, state='disabled')
                else:
                    menu.add_command(label='Restore', command=self.restore, state='normal')

            if self.tree.tag_has('attachments', item):

                if read_only:
                    menu.add_command(label='Add', command=self.add_attachment, state='disabled')
                else:
                    menu.add_command(label='Add', command=self.add_attachment, state='normal')

            if self.tree.tag_has('attachments_children', item):
                menu.add_separator()
                menu.add_command(label='Download', command=self.download_attachment)

                if read_only:
                    menu.add_command(label='Remove', command=self.remove_attachment, state='disabled')
                else:
                    menu.add_command(label='Remove', command=self.remove_attachment, state='normal')

            menu.post(event.x_root + 1, event.y_root)
        else:
            pass

    def new_batch(self):
        dialog = NewBatchDialog(self, 'New Batch')

        if dialog.data:
            batch_name, comment = dialog.data
            files = filedialog.askopenfilenames(title='Select file/s', filetypes=(('jpeg files', '*.pch'), ('all files', '*.*')), parent=self.root)

            if files:

                try:
                    self.database.new_batch(files, batch_name, comment)
                    self.update_tree()
                except Exception as e:
                    self.statusbar.refresh(e)

    def restore(self):
        dialog = RestoreDialog(self, self.tree.focus().split('::')[-1],
                               self.database.header.batches, 'Restore')

        if dialog.data:
            batch_name = dialog.data

            try:
                self.database.restore(batch_name)
                self.update_tree()
            except Exception as e:
                self.statusbar.refresh(e)

    def add_attachment(self):

        try:
            file = filedialog.askopenfilename(title='Select file', parent=self.root)

            if file:
                self.database.add_attachment(file)
                self.update_tree()

        except Exception as e:
            self.statusbar.refresh(e)

    def download_attachment(self):

        try:
            folder = filedialog.askdirectory(parent=self.root)

            if folder:
                self.database.download_attachment(self.tree.focus().split('::')[-1], folder)
                self.update_tree()

        except Exception as e:
            self.statusbar.refresh(e)

    def remove_attachment(self):

        try:
            answer = messagebox.askokcancel('Remove Attachment', 'Proceed?', parent=self.root)

            if answer:
                self.database.remove_attachment(self.tree.focus().split('::')[-1])
                self.update_tree()

        except Exception as e:
            self.statusbar.refresh(e)


class Dialog(Toplevel):

    def __init__(self, parent, title=None):
        super().__init__(parent)
        self.transient(parent)
        self.resizable(False, False)

        if title:
            self.title(title)

        self.parent = parent
        self.data = None

        self.frame = ttk.Frame(self)
        self.frame.pack()

        body = ttk.Frame(self.frame)
        self.initial_focus = self.body(body)
        body.grid(row=0, padx=10, pady=10)

        self.buttonbox()

        self.grab_set()

        if not self.initial_focus:
            self.initial_focus = self

        self.protocol('WM_DELETE_WINDOW', self.cancel)

        self.geometry(f'+{parent.winfo_rootx() + 50}+{parent.winfo_rooty() + 50}')

        self.initial_focus.focus_set()

        self.wait_window(self)

    def body(self, parent):
        # create dialog body.  return widget that should have
        # initial focus.  this method should be overridden
        pass

    def buttonbox(self):
        # add standard button box. override if you don't want the
        # standard buttons
        box = ttk.Frame(self.frame)
        w = ttk.Button(box, text='OK', command=self.ok, default=ACTIVE)
        w.pack(side=LEFT, padx=5, pady=5)
        w = ttk.Button(box, text='Cancel', command=self.cancel)
        w.pack(side=LEFT, padx=5, pady=5)
        self.bind('<Return>', self.ok)
        self.bind('<Escape>', self.cancel)
        box.grid(row=1, padx=5, pady=5)

    def ok(self, event=None):

        if not self.validate():
            self.initial_focus.focus_set() # put focus back
            return

        self.withdraw()
        self.update_idletasks()
        self.apply()
        self.cancel()

    def cancel(self, event=None):
        # put focus back to the parent window
        self.parent.focus_set()
        self.destroy()

    def validate(self):
        return 1 # override

    def apply(self):
        pass # override


class ConnectDialog(Dialog):

    def body(self, parent):
        ttk.Label(parent, text='Address:').grid(row=0, sticky='E')
        ttk.Label(parent, text='User:').grid(row=2, sticky='E')
        ttk.Label(parent, text='Password:').grid(row=3, sticky='E')
        parent.grid_rowconfigure(1, minsize=10)

        self.e1 = ttk.Entry(parent, takefocus=True)
        self.e2 = ttk.Entry(parent)
        self.e3 = ttk.Entry(parent, show='*')

        self.e1.grid(row=0, column=1)
        self.e2.grid(row=2, column=1)
        self.e3.grid(row=3, column=1)
        return self.e1 # initial focus

    def apply(self):
        self.data = (self.e1.get(), self.e2.get(), self.e3.get())

    def validate(self):

        if not self.e1.get() or not self.e2.get() or not self.e3.get():
            return False
        else:
            return True


class NewBatchDialog(Dialog):

    def body(self, parent):
        ttk.Label(parent, text='Name:').grid(row=0, sticky='E')
        ttk.Label(parent, text='Comment:').grid(row=1, sticky='E')

        self.e1 = ttk.Entry(parent, takefocus=True)
        self.e2 = ttk.Entry(parent)

        self.e1.grid(row=0, column=1)
        self.e2.grid(row=1, column=1)
        return self.e1 # initial focus

    def apply(self):
        self.data = (self.e1.get(), self.e2.get())

    def validate(self):

        if not self.e1.get():
            return False
        else:
            return True


class RestoreDialog(Dialog):

    def __init__(self, parent, batch_name, batches, title=None):
        self.batch_name = batch_name
        self.batches = batches
        super().__init__(parent, title)

    def body(self, parent):
        ttk.Label(parent, text='Batch:').grid(row=0, column=0, sticky='E')

        self.e1 = ttk.Combobox(parent, state='readonly',
                               values=[batch[0] for batch in self.batches])

        self.e1.grid(row=0, column=1)
        self.e1.set(self.batch_name)
        return self.e1 # initial focus

    def apply(self):
        self.data = self.e1.get()


class LoadRemoteDatabaseDialog(Dialog):

    def __init__(self, parent, databases, title=None):
        self.databases = databases
        super().__init__(parent, title)

    def body(self, parent):
        ttk.Label(parent, text='Database:').grid(row=0, column=0, sticky='E')

        self.e1 = ttk.Combobox(parent, state='readonly', values=self.databases)

        self.e1.grid(row=0, column=1)
        self.e1.set(self.databases[0])
        return self.e1 # initial focus

    def apply(self):
        self.data = self.e1.get()
