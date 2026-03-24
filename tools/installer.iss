; Instalador local simple (sin TUFUP/S3/Supabase)

#define MyAppName "TerralixERP"
#define MyAppVersion "1.1.0"
#define MyAppPublisher "TerralixERP Spa"
#define MyAppExeName "TERRALIX.exe"

[Setup]
AppId={{D172B9C0-9AE3-49C8-A970-3D05A7A93ABA}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
DefaultDirName={autopf}\{#MyAppName}
UninstallDisplayIcon={app}\{#MyAppExeName}
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
DisableProgramGroupPage=yes
OutputDir={#SourcePath}..\dist\installer
OutputBaseFilename=TerralixERP_Setup
SetupIconFile={#SourcePath}..\Terralix_Logo.ico
SolidCompression=yes
WizardStyle=modern

[Languages]
Name: "spanish"; MessagesFile: "compiler:Languages\\Spanish.isl"
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "..\dist\TERRALIX\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\dist\TERRALIX\_internal\*"; DestDir: "{app}\_internal"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent
