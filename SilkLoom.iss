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

[Code]
var
	PurgeUserDataOnUninstall: Boolean;

function UninstallPurgeDataCheckboxText: String;
begin
	if ActiveLanguage = 'chinesesimp' then
		Result := '卸载时同时删除用户数据（配置、缓存、日志、导出）'
	else
		Result := 'Also remove user data on uninstall (config, cache, logs, exports)';
end;

function UninstallPromptText: String;
begin
	if ActiveLanguage = 'chinesesimp' then
		Result := '请确认是否卸载 SilkLoom。'
	else
		Result := 'Please confirm uninstalling SilkLoom.';
end;

function UninstallConfirmButtonText: String;
begin
	if ActiveLanguage = 'chinesesimp' then
		Result := '卸载'
	else
		Result := 'Uninstall';
end;

function UninstallCancelButtonText: String;
begin
	if ActiveLanguage = 'chinesesimp' then
		Result := '取消'
	else
		Result := 'Cancel';
end;

function ShowUninstallOptionsDialog(var PurgeData: Boolean): Boolean;
var
	Form: TSetupForm;
	InfoLabel: TNewStaticText;
	DataCheckBox: TNewCheckBox;
	UninstallBtn: TNewButton;
	CancelBtn: TNewButton;
	DialogResult: Integer;
begin
	Form := CreateCustomForm(ScaleX(440), ScaleY(150), False, True);
	try
		Form.Caption := ExpandConstant('{#AppName}');

		InfoLabel := TNewStaticText.Create(Form);
		InfoLabel.Parent := Form;
		InfoLabel.Left := ScaleX(16);
		InfoLabel.Top := ScaleY(16);
		InfoLabel.Width := Form.ClientWidth - ScaleX(32);
		InfoLabel.Height := ScaleY(24);
		InfoLabel.Caption := UninstallPromptText;

		DataCheckBox := TNewCheckBox.Create(Form);
		DataCheckBox.Parent := Form;
		DataCheckBox.Left := ScaleX(16);
		DataCheckBox.Top := ScaleY(54);
		DataCheckBox.Width := Form.ClientWidth - ScaleX(32);
		DataCheckBox.Height := ScaleY(24);
		DataCheckBox.Caption := UninstallPurgeDataCheckboxText;
		DataCheckBox.Checked := False;

		UninstallBtn := TNewButton.Create(Form);
		UninstallBtn.Parent := Form;
		UninstallBtn.Caption := UninstallConfirmButtonText;
		UninstallBtn.ModalResult := mrOk;
		UninstallBtn.Left := Form.ClientWidth - ScaleX(200);
		UninstallBtn.Top := Form.ClientHeight - ScaleY(38);
		UninstallBtn.Width := ScaleX(88);

		CancelBtn := TNewButton.Create(Form);
		CancelBtn.Parent := Form;
		CancelBtn.Caption := UninstallCancelButtonText;
		CancelBtn.ModalResult := mrCancel;
		CancelBtn.Left := Form.ClientWidth - ScaleX(104);
		CancelBtn.Top := Form.ClientHeight - ScaleY(38);
		CancelBtn.Width := ScaleX(88);

		Form.ActiveControl := UninstallBtn;
		DialogResult := Form.ShowModal;
		PurgeData := DataCheckBox.Checked;
		Result := DialogResult = mrOk;
	finally
		Form.Free;
	end;
end;

procedure AddPathIfMissing(var Paths: TArrayOfString; const Candidate: String);
var
	I: Integer;
begin
	if Candidate = '' then
		exit;

	for I := 0 to GetArrayLength(Paths) - 1 do
		if CompareText(Paths[I], Candidate) = 0 then
			exit;

	SetArrayLength(Paths, GetArrayLength(Paths) + 1);
	Paths[GetArrayLength(Paths) - 1] := Candidate;
end;

function GetEnvOrEmpty(const Key: String): String;
begin
	Result := GetEnv(Key);
	if Result = '' then
		Result := GetEnv('=' + Key);
end;

procedure PurgeUserDataRoots;
var
	Paths: TArrayOfString;
	I: Integer;
	TargetPath: String;
	DeleteOk: Boolean;
begin
	SetArrayLength(Paths, 0);

	AddPathIfMissing(Paths, ExpandConstant('{userdocs}\SilkLoom'));
	AddPathIfMissing(Paths, GetEnvOrEmpty('SILKLOOM_DATA_DIR'));
	AddPathIfMissing(Paths, GetEnvOrEmpty('SILKLOOM_CONFIG_DIR'));

	for I := 0 to GetArrayLength(Paths) - 1 do begin
		TargetPath := RemoveBackslashUnlessRoot(Paths[I]);
		if (TargetPath <> '') and DirExists(TargetPath) then begin
			Log(Format('Purge user data: deleting "%s"', [TargetPath]));
			DeleteOk := DelTree(TargetPath, True, True, True);
			if not DeleteOk then
				Log(Format('Purge user data: failed to delete "%s"', [TargetPath]));
		end else
			Log(Format('Purge user data: path not found, skipped "%s"', [TargetPath]));
	end;
end;

function InitializeUninstall: Boolean;
begin
	PurgeUserDataOnUninstall := False;
	Result := True;

	if not UninstallSilent then
		Result := ShowUninstallOptionsDialog(PurgeUserDataOnUninstall);
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
	if CurUninstallStep = usUninstall then begin
		if PurgeUserDataOnUninstall then
			PurgeUserDataRoots;
	end;
end;
