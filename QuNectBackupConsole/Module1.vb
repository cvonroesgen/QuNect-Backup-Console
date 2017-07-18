Imports System.Net
Imports System.IO
Imports System.Text
Imports System.Data.Odbc
Imports System.Text.RegularExpressions
Module Module1

    Sub Main(ByVal arguments As String())

        If arguments.Length < 2 Then
            Console.WriteLine("Please supply three command line arguments:")
            Console.WriteLine("QuNectBackupConsole ""32 bit DSN"" ""Path\to\backup\folder"" period.delimited.dbids")
            Exit Sub
        End If
        'here we need to go through the list and backup
        Dim i As Integer
        Dim connectionString As String = "DSN=" & arguments(0) & ";"

        Dim quNectConn As OdbcConnection = New OdbcConnection(connectionString)

        Try
            quNectConn.Open()
        Catch excpt As Exception
            Console.WriteLine(excpt.Message & vbCrLf & connectionString)
            quNectConn.Dispose()
            Exit Sub
        End Try

        Dim stringSeparators() As String = {","}
        Dim dbids() As String
        dbids = arguments(2).Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries)

        For i = 0 To dbids.Count - 1
            If backupTable(dbids(i), dbids(i), quNectConn, arguments(1)) Then
                Exit For
            End If
        Next
        quNectConn.Close()
        quNectConn.Dispose()
        If dbids.Count = 1 Then
            Console.WriteLine("Your table has been backed up!")
        Else
            Console.WriteLine("Your tables have been backed up!")
        End If

    End Sub
    Private Function backupTable(ByVal dbName As String, ByVal dbid As String, ByVal quNectConn As OdbcConnection, folderPath As String) As Boolean
        'we need to get the schema of the table
        Dim restrictions(2) As String
        restrictions(2) = dbid
        Dim columns As DataTable = quNectConn.GetSchema("Columns", restrictions)
        'now we can look for formula fileURL fields
        backupTable = True
        Dim quickBaseSQL As String = "select count(1) from """ & dbid & """"

        Dim quNectCmd As OdbcCommand = New OdbcCommand(quickBaseSQL, quNectConn)
        Dim dr As OdbcDataReader
        Try
            dr = quNectCmd.ExecuteReader()
        Catch excpt As Exception
            quNectCmd.Dispose()
            Exit Function
        End Try
        If Not dr.HasRows Then
            Exit Function
        End If

        Dim recordCount As Integer = dr.GetValue(0)
        quNectCmd.Dispose()

        quickBaseSQL = "select fid, field_type, formula, mode from """ & dbid & "~fields"""

        quNectCmd = New OdbcCommand(quickBaseSQL, quNectConn)
        Try
            dr = quNectCmd.ExecuteReader()
        Catch excpt As Exception
            quNectCmd.Dispose()
            Exit Function
        End Try
        If Not dr.HasRows Then
            Exit Function
        End If




        Dim i
        Dim clist As String = ""
        Dim fieldTypes As String = ""
        Dim period As String = ""
        While (dr.Read())
            Dim mode As String = dr.GetString(3)
            Dim formula As String = dr.GetString(2)
            Dim field_type As String = dr.GetString(1)
            If (field_type = "url" Or field_type = "dblink") And mode = "virtual" And Not formula.Contains("/AmazonS3/download.aspx?") Then
                Continue While
            End If
            clist &= period & dr.GetString(0)
            fieldTypes &= period & field_type
            period = "."
        End While
        quNectCmd.Dispose()


        Directory.CreateDirectory(folderPath)
        Dim filenamePrefix As String = dbName.Replace("/", "").Replace("\", "").Replace(":", "").Replace(":", "_").Replace("?", "").Replace("""", "").Replace("<", "").Replace(">", "").Replace("|", "")
        If filenamePrefix.Length > 229 Then
            filenamePrefix = filenamePrefix.Substring(filenamePrefix.Length - 229)
        End If
        Dim filepath As String = folderPath & "\" & filenamePrefix & ".fids"
        Dim objWriter As System.IO.StreamWriter
        Try
            objWriter = New System.IO.StreamWriter(filepath)
        Catch excpt As Exception
            Exit Function
        End Try
        objWriter.Write(clist & vbCrLf & fieldTypes)
        objWriter.Close()

        'here we need to open a file
        'filename prefix can only be 229 characters in length
        quickBaseSQL = "select * from """ & dbid & """"

        quNectCmd = New OdbcCommand(quickBaseSQL, quNectConn)

        Try
            dr = quNectCmd.ExecuteReader()
        Catch excpt As Exception
            quNectCmd.Dispose()
            Exit Function
        End Try
        If Not dr.HasRows Then
            Exit Function
        End If

        filepath = folderPath & "\" & filenamePrefix & ".csv"
        Try
            objWriter = New System.IO.StreamWriter(filepath)
        Catch excpt As Exception
            Exit Function
        End Try

        For i = 0 To dr.FieldCount - 1
            objWriter.Write("""")
            objWriter.Write(Replace(CStr(dr.GetName(i)), """", """"""))
            objWriter.Write(""",")
        Next

        objWriter.Write(vbCrLf)
        Dim k As Integer = 0
        While (dr.Read())
            Console.WriteLine("Backing up " & k & " of " & recordCount)
            k += 1
            For i = 0 To dr.FieldCount - 1
                If dr.GetValue(i) Is Nothing Or IsDBNull(dr.GetValue(i)) Then
                    objWriter.Write(",")
                Else
                    Dim strCell As String = dr.GetValue(i).ToString()
                    objWriter.Write("""")
                    objWriter.Write(Replace(strCell, """", """"""))
                    objWriter.Write(""",")
                End If
            Next
            objWriter.Write(vbCrLf)
        End While
        objWriter.Close()
        dr.Close()
        quNectCmd.Dispose()
    End Function

End Module
