# -*- mode: python -*-

block_cipher = None


a = Analysis(['app.py'],
             pathex=[r'C:\Alvaro\nastran_tools\loadit\loadit\scripts'],
             binaries=[],
             datas=[],
             hiddenimports=['pandas', 'pyarrow.formatting'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
a.datas += [('icon.png',r'C:\Alvaro\nastran_tools\loadit\loadit\scripts\icon.png','DATA')]
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='Loadit',
          debug=False,
          strip=False,
          upx=True,
          console=False,
          icon=r'C:\Alvaro\nastran_tools\loadit\loadit\scripts\icon.ico')
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='Loadit')
