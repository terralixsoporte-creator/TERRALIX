; ============================================================
; Inno Setup Script for Terralix ERP
; ============================================================
; Requires: Inno Setup 6.x  (https://jrsoftware.org/isinfo.php)
;
; Build steps:
;   1. pyinstaller TERRALIX.spec          (generates dist\TERRALIX\)
;   2. Open this file in Inno Setup Compiler and click "Compile"
;      OR run:  iscc installer.iss
; ============================================================

#define MyAppName      "Terralix ERP"
#define MyAppVersion   "1.1.0"
#define MyAppPublisher "Terralix"
#define MyAppExeName   "TERRALIX.exe"
#define MyAppURL       "https://terralix.cl"

[Setup]
AppId={{A3F7B2C1-4D5E-6F78-9A0B-C1D2E3F4A5B6}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppVerName={#MyAppName} v{#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
DisableProgramGroupPage=yes
OutputDir=dist\installer
OutputBaseFilename=TerralixERP_Setup
SetupIconFile=Terralix_Logo.ico
UninstallDisplayIcon={app}\TERRALIX.exe
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
; Copia TODO lo que genero PyInstaller (exe + _internal + futuros recursos)
Source: "dist\TERRALIX\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

; Data files that should live beside the app (writable)
Source: "data\Manual_Terralix_ERP.pdf"; DestDir: "{app}\data"; Flags: ignoreversion

[Dirs]
; Carpeta de datos de usuario escribible en AppData (config.env, logs, etc.)
Name: "{userappdata}\Terralix ERP"; Permissions: users-modify

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\TERRALIX.exe"
Name: "{group}\Desinstalar {#MyAppName}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "Iniciar {#MyAppName}"; Flags: nowait postinstall skipifsilent shellexec
