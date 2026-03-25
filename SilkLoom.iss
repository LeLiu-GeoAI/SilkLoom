#define AppName "SilkLoom"
#define AppVersion GetEnv("VERSION")

[Setup]
AppId={{7B5CE8CB-D0BC-4C4A-8877-B4D85F0382C6}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher=SilkLoom
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
OutputDir=dist
OutputBaseFilename=SilkLoom-v{#AppVersion}-Windows-x86_64-Setup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
SetupIconFile=assets\icons\logo.ico
UninstallDisplayIcon={app}\SilkLoom.exe

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "chinesesimp"; MessagesFile: "compiler:Languages\ChineseSimplified.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "dist\main.dist\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "assets\icons\logo.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\SilkLoom"; Filename: "{app}\SilkLoom.exe"; IconFilename: "{app}\logo.ico"; IconIndex: 0
Name: "{autodesktop}\SilkLoom"; Filename: "{app}\SilkLoom.exe"; IconFilename: "{app}\logo.ico"; IconIndex: 0; Tasks: desktopicon

[Run]
Filename: "{app}\SilkLoom.exe"; Description: "{cm:LaunchProgram,SilkLoom}"; Flags: nowait postinstall skipifsilent
