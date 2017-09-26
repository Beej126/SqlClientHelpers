
### Proc class members 
*Nutshell: SqlCommand convenience wrapper*

### Benefits
* automatically populates proc parms collection by hitting sql server so callers don't have to bother with that unnecessary boilerplate code
* sanity inducing conventions like consistent empty-string=null handling on input parms
* handy extensions like direct output to Json (including optional spacing of property names by title casing)
* and one liner proc parm assignment by standard collections like Dictionary and NameValueCollection

### Peculiarities
* one mildly screwy thing, for deployment convenience, is Newtonsoft.Json.dll and Humanizer.dll are embedded resources of SqlClientHelpers.dll.

#### Minimal sample usage:
```C#
Proc.ConnectionStringDefault = ""; //one time only
var proc = new Proc("procname");
proc["@Parm1"] = "";
var ds = proc.ExecuteDataset();
var getOutParm = proc["@Parm2"]; 
```

#### Config
* static string ConnectionStringDefault
* static int CommandTimeoutSeconds = 30
* static Action<Exception> OnErrorDefault
* Action<Exception> OnError
* static Action OnSuccessDefault
* Action OnSuccess

#### Execution
* DataSet DataSet
* DataSet ExecuteDataSet()
* async Task<DataSet> ExecuteDataSetAsync()
* SqlDataReader ExecuteReader(bool blobMode = false)
* void **ExecuteJson**(Stream, bool titleCasePropertyNames = false)
* async Task<bool> ExecuteNonQueryAsync()

#### Parameter related
* object this[string key]
* static void ResetParmcache()
* SqlParameterCollection Parms
* Proc AssignParms(IDictionary<string, object>)
* Proc AssignParms(NameValueCollection)
* Proc AssignParms(DataRowView)
* Proc AssignParms(DataRow)
* Proc AssignParms(object[])
* T GetParmAs<T>(string key)
* void ClearParms()

#### Utility
* string[] TableNames
* string[] MatchingTableNames(params string[])
* static Func<IDisposable> NewWaitObject
* string SQLWarnings
* bool TrimAndNull = true

### SqlClientHelpers members
*General ADO.Net helpers*
* static DataTable Table0(this DataSet)
* static DataRow Row0(this DataSet)
* static long? AsLong(this SqlParameter)
* static string AsString(this SqlParameter)
* static IEnumerable<Dictionary<string, object>> ToDictionaryRows(this DataTable)
* static async Task ToStreamAsync(this SqlDataReader, Stream, string fieldName = null)
* static void ToJson(this SqlDataReader, Stream, bool titleCasePropertyNames = false)
* static NameValueCollection ToNameValueCollection(this DataTable, NameValueCollection vals = null)
* static string BuildRowFilter(params string[] filters)
* static void SetReadonlyField(this DataRow, string colName, object data)
* static T Field<T>(this DataRowView, string fieldName)
* static bool IsFieldsModified(this DataRow, params string[] fieldNames)
* static void SetAllNonDbNullColumnsToEmptyString(this DataRow, string emptyPlaceholder = "")
* static void RemoveEmptyPlaceholder(this DataRow, string emptyPlaceholder)
* static void SetColumnError(this DataRowView, string fieldName, string message)
* static void ClearColumnError(this DataRowView, string fieldName)
* static bool ColumnsContain(this DataRowView, string columnName)
* static void AcceptChanges(this DataRowView)
* static void AcceptChanges(this DataView)
* static bool IsDirty(this DataRowView)
* static bool IsDirty(this DataView, bool respectRowFilter = true)
* static void DetachRowsAndDispose(this DataView, bool preserveRowFilter = false)
* static void DetachRow(this DataRowView)
* static void AddRelation(this DataSet, string name, DataColumn parent, DataColumn child,
* static void AddColumn(this DataTable, string colName, Type type, string expression)
* static DataRow Clone(this DataRow)
* static string SqlErrorTextCleaner(string message)
* static void AddInitialEmptyRows(DataSet, StringCollection rootTables)
* static DataRow AddNewRowWithPk(DataTable)
* static DataRow AddNewNestedRow(DataTable)
