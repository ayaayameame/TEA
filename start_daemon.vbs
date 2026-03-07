' =========================================================================
' Project TEA - Background Daemon Starter (Hardened Version)
' Targets: S:\Project\TEA
' =========================================================================

Set WshShell = CreateObject("WScript.Shell")

' --- 1. Define Project Paths ---
' Using absolute paths to ensure reliability on Drive S
projectRoot = "S:\Project\TEA"
pythonExe   = "S:\Project\TEA\.venv\Scripts\python.exe"
scriptPath  = "S:\Project\TEA\src\fetcher\run_fetcher.py"

' --- 2. Construct the Execution Command ---
' In VBS, to include a double quote inside a string, you must use "" (double double-quotes)
' This ensures the final command looks like: "S:\path\to\python.exe" "S:\path\to\script.py"
strCommand = """" & pythonExe & """ """ & scriptPath & """"

' --- 3. Execute with Current Directory Context ---
' Setting CurrentDirectory ensures logs and database are created in the correct folder
On Error Resume Next
WshShell.CurrentDirectory = projectRoot
If Err.Number <> 0 Then
    MsgBox "Error: Could not find directory " & projectRoot, 16, "Project TEA"
    WScript.Quit
End If
On Error GoTo 0

' Run the command: 0 = Hide window, False = Do not wait for completion
WshShell.Run strCommand, 0, False

Set WshShell = Nothing