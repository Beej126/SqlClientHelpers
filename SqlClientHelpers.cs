using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data;
using System.Data.SqlTypes;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.ComponentModel;
using System.Reflection;
using System.Xml;

//make sure to keep this clean of any particular UI assembly dependencies so that it can be
//reused across ASP.Net, Windows.Forms and WPF projects

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/// <summary>
/// Basically a wrapper around SqlCommand.
/// One benefit is .Parameters is pre-populated and conveniently exposed as Proc[string] indexer.
/// </summary>
// ReSharper disable CheckNamespace
public class Proc : IDisposable
// ReSharper restore CheckNamespace
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
{
  #region Wait Cursor stuff
  public delegate IDisposable WaitObjectConstructor();
  static public WaitObjectConstructor NewWaitObject = DummyWaitObjectConstructor;
  static private IDisposable DummyWaitObjectConstructor()
  {
    return (new DummyDisposable());
  }
  private class DummyDisposable : IDisposable { public void Dispose() { } };
  #endregion

  public delegate void ExecuteMessageCallback(string message);
  static public ExecuteMessageCallback MessageCallback;

  /// <summary>
  /// Default is 30 secs
  /// </summary>
  public static int CommandTimeoutSeconds = 30;

  private void DoMessageCallback(string message, string prefix = null)
  {
    if (message == null) return;

    if (MessageCallback != null) MessageCallback(prefix + SqlClientHelpers.SqlErrorTextCleaner(message));
    else throw (new Exception(message));
  }

  private void DoMessageCallback(Exception ex, string prefix = null)
  {
    if (ex == null) return;

    if (MessageCallback != null) DoMessageCallback(ex.Message, prefix);
    else throw (prefix == null ? ex : new Exception(prefix + ex.Message)); //the context info provided by Prefix seems important enough to toss all the other exception info in the balance... we could probably get a little more fancy here if that info becomes crucial
  }

  public string PrintOutput { get; private set; }

  /// <summary>
  /// Either a .config Named Connection String, or full Connection String
  /// </summary>
  static public string DefaultConnectionString;
  protected string ConnString;

  public bool TrimAndNull = true;
  private SqlCommand _cmd;
  private readonly string _procName;
  //private readonly string _userName;
  private DataSet _ds;

  private bool _procOwnsDataSet = true;
  public DataSet DataSet { get { return (_ds); } set { _ds = value; _procOwnsDataSet = false; } }

  public Proc(string procName) : this(procName, null) { }

  public bool IsDevMode { get { return (Process.GetCurrentProcess().ProcessName == "devenv"); } }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="procName"></param>
  /// <param name="connectionString">Pass either full connectiong string or named connection string from .config</param>
  /* <param name="userName">pass this in to set "Workstation ID" which can be obtained via T-SQL's HOST_NAME() function... as a handy layup for a simple table audit framework :)</param>
  // I'm not entirely convinced this is the most elegant way to support this versus something more "automatic" in the background
  // the main challenge is maintaining a generically reusable Proc class that doesn't know whether it's running under ASP.Net or WPF
  // so rather than implementing a bunch of dynamic "drilling" to identify where you are and who the current username is
  // i'm thinking this is a nice "good enough" for now to simply pass it in from the outercontext*/
  public Proc(string procName, string connectionString)
  {
    ConnString = connectionString ?? DefaultConnectionString;
    var configConn = ConfigurationManager.ConnectionStrings[ConnString];
    if (configConn != null) ConnString = configConn.ConnectionString;

    _procName = procName;
    //_userName = userName;
    //scratch that idea... see "NOOP" comment further down: if (_ProcName == "NOOP") return; //this supports doing nothing if we're chaining AssignValues(DataRowView).ExecuteNonQuery() and the DataRowView.Row.RowState == UnChanged

    if (IsDevMode) return;

    Debug.Assert(ConnString != null, 
      "Proc.DefaultConnectionString or new Proc(name, connectionString) must be specified. System.ComponentModel.LicenseManager.UsageMode: " + 
      LicenseManager.UsageMode.ToString() + ", System.Diagnostics.Process.GetCurrentProcess().ProcessName: " + 
      Process.GetCurrentProcess().ProcessName);

    if (!_procName.Contains('.')) _procName = "dbo." + _procName;

    PopulateParameterCollection();
  }

  static Proc()
  {
    ResetParmcache();
  }
  static private readonly Regex UdtParamTypeNameFix = new Regex(@"(.*?)\.(.*?)\.(.*)", RegexOptions.Compiled);
  private static Dictionary<string, SqlCommand> _parmcache;
  static public void ResetParmcache()
  {
    _parmcache = new Dictionary<string, SqlCommand>(StringComparer.OrdinalIgnoreCase);
  }

  public SqlParameterCollection Parms { get { return (_cmd.Parameters); } }

  public string[] TableNames { 
    get { 
      Debug.Assert(DataSet != null, String.Format("Proc.TableNames pulled prior to populating dataset (procname: {0})", _procName));
      return (DataSet.Tables.Cast<DataTable>().Select(t => t.TableName).ToArray());
    }
  }

  public string[] MatchingTableNames(params string[] baseTableNames)
  {
    if (baseTableNames.Length == 0) return (TableNames); //if no table name filter specified, then we want all of them
    return(TableNames.Where(have => baseTableNames.Any(want => //nugget: compare two lists with linq and take the intersection, 
      Regex.IsMatch(have, want + @"(\.[0-9]+)?$", RegexOptions.IgnoreCase))).ToArray()); //nugget: with a Regex for the comparison <nice>

    /* this was a fun learning exercise, but there's too much ambiguity involved with IEqualityComparer's dependence on having a rational GetHashCode() available
     * see this: http://stackoverflow.com/questions/98033/wrap-a-delegate-in-an-iequalitycomparer
     * so, sticking with the nested Linq approach above, works just fine and there's no screwy edge cases to worry about

     OnlyTableNames = OnlyTableNames.Intersect(proc.TableNames, //for an even more Linq'y approach, use built in .Intersect() ... 
        new LambdaComparer<string>((have, want) => //but we need a helper class to wrapper the required IEqualityComparer<T> ...
          Regex.IsMatch(have, want + @"(\.[0-9]+)?$", RegexOptions.IgnoreCase))).ToArray(); //around a comparison lambda */
  }

  private void PopulateParameterCollection(bool refreshCache = false)
  {
    //pull cached parms if available
    var logicalConnectionString = ConnString; //+ ((_userName != null) ? ";Workstation ID=" + _userName : "");
    var parmcachekey = logicalConnectionString + "~" + _procName;

    //this is used to facilitate an automatic parm load retry when we run into a "parameter missing" exception
    if (refreshCache) { _parmcache.Remove(parmcachekey); return; }
        
    var hasCachedParms = _parmcache.TryGetValue(parmcachekey, out _cmd);
    _cmd = hasCachedParms ? _cmd.Clone() : new SqlCommand(_procName) {CommandType = CommandType.StoredProcedure};
    _cmd.CommandTimeout = CommandTimeoutSeconds;
    _cmd.Connection = new SqlConnection(logicalConnectionString);
    if (_cmd.Connection.State != ConnectionState.Open) _cmd.Connection.Open();

    if (!hasCachedParms)
    {
      //i love this little gem, 
      //this allows us to skip all that noisy boiler plate proc parm definition code in the calling context, and simply assign parm names to values 
      SqlCommandBuilder.DeriveParameters(_cmd); //nugget: automatically assigns all the available parms to this SqlCommand object by querying SQL Server's proc definition metadata

      //strip the dbname off any UDT's... there appears to be a mismatch between the part of microsoft that wrote DeriveParameters and what SQL Server actually wants
      //otherwise you get this friendly error message:
      //The incoming tabular data stream (TDS) remote procedure call (RPC) protocol stream is incorrect. Table-valued parameter 1 ("@MyTable"), row 0, column 0: Data type 0xF3 (user-defined table type) has a non-zero length database name specified.  Database name is not allowed with a table-valued parameter, only schema name and type name are valid.
      foreach (SqlParameter p in _cmd.Parameters) if (p.TypeName != "")
        {
          Match m = UdtParamTypeNameFix.Match(p.TypeName);
          if (m.Success) p.TypeName = m.Groups[2] + "." + m.Groups[3];
        }

      _parmcache.Add(parmcachekey, _cmd.Clone()); //nugget: cache SqlCommand objects to avoid unnecessary SqlCommandBuilder.DeriveParameters() calls
    }
  }

  public void AssignValues(NameValueCollection values)
  {
    foreach (string key in values.Keys)
    {
      if (_cmd.Parameters.Contains("@" + key))
        this["@" + key] = values[key];
    }
  }

  public Proc AssignValues(DataRowView values) 
  {
    return (AssignValues(values.Row));
  }

  public Proc AssignValues(DataRow values) //nugget: giving the "fluent" API approach a shot: http://en.wikipedia.org/wiki/Fluent_interface, well ok, starting with simple method chaining then: http://martinfowler.com/bliki/FluentInterface.html
  {
    //don't do this, then we can't chain multiple things together where one may be dirty at the end!! if (values.Row.RowState == DataRowState.Unchanged) return new Proc("NOOP");

    foreach (DataColumn col in values.Table.Columns)
    {
      string colname = "@" + col.ColumnName;

      if (!_cmd.Parameters.Contains(colname))
        colname = "@" + col.ColumnName.Replace(" ", ""); //try mapping to columns by removing spaces in the name

      if (!_cmd.Parameters.Contains(colname)) continue; //otherwise it's not here, just move on, the approach is that this is not supposed to be a show stopper

      this[colname] = values[col.ColumnName];
    }
    return (this);
  }


  public Proc AssignValues(object[] values)
  {
    for(int i = 0; i < values.Length; i += 2)
    {
      if (_cmd.Parameters.Contains("@" + values[i]))
        this["@" + values[i]] = values[i+1];
    }
    return (this);
  }

  public void Dispose()
  {
    if (_cmd != null)
    {
      _cmd.Connection.InfoMessage -= ConnectionInfoMessage;
      _cmd.Connection.Dispose();
      _cmd.Connection = null;
      _cmd.Dispose();
      _cmd = null;
    }

    if (_ds !=null && _procOwnsDataSet) _ds.Dispose();
    _ds = null;
  }

  public Proc ExecuteDataSet()
  {
    if (IsDevMode) return (this); 
    //this doesn't work like it should: if (System.ComponentModel.LicenseManager.UsageMode == System.ComponentModel.LicenseUsageMode.Designtime) return(null);

    using (NewWaitObject())
    using (var da = new SqlDataAdapter(_cmd))
    {
      if (_ds == null) _ds = new DataSet();
      _ds.EnforceConstraints = false;
      if (_cmd.Connection.State != ConnectionState.Open) _cmd.Connection.Open();
      _cmd.Connection.InfoMessage += ConnectionInfoMessage; //nugget: capture print/raiserror<11 output
      da.MissingSchemaAction = MissingSchemaAction.AddWithKey; //this magical line tells ADO.Net to go to the trouble of bringing back the schema info like DataColumn.MaxLength (which would otherwise always be -1!!)
      //da.FillSchema(_ds, SchemaType.Source);
      try
      {
        da.Fill(_ds);
        DoMessageCallback(PrintOutput);
        NameTables();
      }
      catch (Exception ex)
      {
        DoMessageCallback(ex);
      }
    }

    foreach (DataTable table in _ds.Tables) foreach (DataColumn column in table.Columns) column.ReadOnly = false;
    return (this); //trying some "Fluent API" type stuff: http://en.wikipedia.org/wiki/Fluent_interface
  }

  private void NameTables()
  {
    //establish a nice little convention here... sprocs can return a list of "friendly" names for each resultset...
    //(not required, just supported if the @TableNames output parm is present)
    //so, if the proc is telling us it's list of tables names, rename the generic Table0, Table1, etc to the corresponding names so caller can reference them more explicitly
    if (_cmd.Parameters.Contains("@TableNames"))
    {
      string[] tableNames = this["@TableNames"].ToString().ToLower().CleanCommas().Split(',')
        //we'd like the flexibility to send back multiple resultsets with the *SAME* name
        //this allows us the non trivial freedom to fire nested proc calls which return data headed into the same table without coupling those procs' internal logic with the need to do a union
        //but DataSet doesn't allow duplicate table names
        //so, .Uniquify() loops through the strings and renames them to table.1, table.2 etc
        //(the client will just have to be involved with anticipating this and consuming appropriately)
        .Uniquify().ToArray();

      Debug.Assert(tableNames.Length == _ds.Tables.Count,
        String.Format("{0}[@TableNames] specifies {1} table(s), but proc returned {2} resultsets.",
        _procName, tableNames.Length, _ds.Tables.Count));

      //at this point it's just a matter of renaming the DataSet.Tables by ordinal mapping to our unique list of tablenames...
      for(int i=0; i<tableNames.Length; i++) _ds.Tables[i].TableName = tableNames[i];

      //and lastly, sync the @TableNames parm back up with whatever renaming we've done
      //just in case the caller depends on this being consistent, but preferrably callers will rely on the Proc.TableNames property for this
      this["@TableNames"] = TableNames.Join(","); 
    }
  }

  void ConnectionInfoMessage(object sender, SqlInfoMessageEventArgs e)
  {
    PrintOutput = e.Message;
  }

  public DataSet ExecuteDataSet(object label) //actually any object with a .Text propertyName will suffice :)
  {
    ReflectionHelpers.PropertySetter(label, "Success", true);

    try
    {
      return(ExecuteDataSet().DataSet);
    }
    catch(Exception ex) {
      ReflectionHelpers.PropertySetter(label, "Success", false);

      //if the caller has provided a way to display the error then do so
      if (ReflectionHelpers.PropertySetter(label, "Text", SqlClientHelpers.SqlErrorTextCleaner(ex.Message)))
        return (null);
      //otherwise rethrow so that we can see the buggy that caused the exception and fix it
      throw;
    }
  }

  public bool ExecuteDataSet(String executeMessagePrefix, bool displaySuccess = false)
  {
    try
    {
      ExecuteDataSet();
      if (displaySuccess) DoMessageCallback(executeMessagePrefix == null ? "Saved Successfully" : executeMessagePrefix + "successful");
      return (true);
    }
    catch (Exception ex)
    {
      DoMessageCallback(ex, executeMessagePrefix);
      return (false);
    }
  }

  public Proc ExecuteNonQuery(bool retryContext = false)
  {
    if (_cmd == null) return (this);

    using (NewWaitObject())
    {
      try
      {
        _cmd.ExecuteNonQuery();
      }
      catch (Exception ex)
      {
        //nugget: automatic reload of the proc parms in case we're actively patching procs while running... it was easy to implement here, still need to investigate feasibility of implementing ExecuteDataSet() with this approach
        if (!retryContext && ex.Message.Contains("expects parameter")) //nugget:
        {
          PopulateParameterCollection(refreshCache: true); //nugget:
          ExecuteNonQuery(true); //nugget: retry 
        }
        else throw; 
      }
      return (this);
    }
  }

  public bool ExecuteNonQuery(object label, bool displaySuccess)
  {
    try
    {
      ExecuteNonQuery();
      if (displaySuccess) ReflectionHelpers.PropertySetter(label, "Text", "Saved Successfully");
      return (true);
    }
    catch (Exception ex)
    {
      if (!ReflectionHelpers.PropertySetter(label, "Text", SqlClientHelpers.SqlErrorTextCleaner(ex.Message)))
        throw;
      return (false);
    }
  }

  public void ExecuteNonQueryBackground(String executeMessagePrefix, bool displaySuccess = false)
  {
    (new System.Threading.Thread(delegate()
    {
      try
      {
        ExecuteNonQuery(executeMessagePrefix, displaySuccess);
      }
      catch (Exception ex) //this exception handler should be included as part of this threading pattern wherever else it's implemented so that we don't lose errors behind the scenes
      {
        DoMessageCallback(ex, executeMessagePrefix);
      }
    })).Start();
  }

  public bool ExecuteNonQuery(String executeMessagePrefix, bool displaySuccess = false, bool backgroundThreadContext = false)
  {
    Debug.Assert(MessageCallback != null, "SqlClientHelpers.cs : Proc.MessageCallback should not be null when calling this overload");

    try
    {
      ExecuteNonQuery();
      if (displaySuccess) DoMessageCallback(executeMessagePrefix + "Saved Successfully");
      if (backgroundThreadContext) Dispose();
      return (true);
    }
    catch (Exception ex)
    {
      if (backgroundThreadContext) throw;
      DoMessageCallback(ex, executeMessagePrefix);
      return (false);
    }
  }

  public DataTableCollection Tables
  {
    get
    {
      Debug.Assert(_ds != null, "must execute proc prior to retrieving tables");
      return (_ds.Tables);
    }
  }

  public DataTable Table0
  {
    get
    {
      if (IsDevMode) return(new DataTable());

      if (_ds == null) ExecuteDataSet();
      Debug.Assert(_ds != null, "_ds != null");
      return (_ds.Tables.Count > 0 ? _ds.Tables[0] : null);
    }
  }

  public DataRow Row0
  {
    get
    {
      if (_ds == null) ExecuteDataSet();
      //Debug.Assert(_ds != null, "must execute proc prior to retrieving tables");
      return (Table0 == null || Table0.Rows.Count == 0 ? null : Table0.Rows[0]);
    }
  }

  public SqlDataReader ExecuteReader()
  {
    return (_cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.CloseConnection));
  }

  public NameValueCollection ExecuteNameValueCollection(NameValueCollection vals = null)
  {
    return (SqlClientHelpers.DataTableToNameValueCollection(Table0, vals));
  }

  public T GetParm<T>(string key)
  {
    //taken from DataRowExtensions.Field<T>
    //https://github.com/mosa/Mono-Class-Libraries/blob/master/mcs/class/System.Data.DataSetExtensions/System.Data/DataRowExtensions.cs
    var ret = _cmd.Parameters[key].Value;
    if (ret == DBNull.Value)
    {
      var type = typeof(T);
      var genericTypeDef = type.IsGenericType ? type.GetGenericTypeDefinition() : null;
      if (!type.IsValueType || genericTypeDef != null && genericTypeDef == typeof(Nullable<>))
        return default(T);

      throw new StrongTypingException("Cannot get strong typed value since it is DB null. Please use a nullable type.", null);
    }

    //http://stackoverflow.com/questions/2961656/generic-tryparse
    try
    {
      return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFromString(ret.ToString());
    }
    catch
    {
      return default(T);
    }
  }
  public object this[string key]
  {
    //nulls can get a little tricky to look at here...
    //if TrimAndNull is on, then it'll truncate a blank string and convert it to DBNull

    //this getter here returns SqlValue not "Value" ... which translates SqlString parameters containing DBNull into C# nulls... 
    //this may or may not come in handy... we'll have to see and tweak accordingly
    get
    {
      return(_cmd.Parameters[key].Value);
    }
    set
    {
      if (IsDevMode || !_cmd.Parameters.Contains(key)) return;

      if (TrimAndNull && (value != null) && (value != DBNull.Value) && (value is string || value is SqlChars || value is SqlString))
      {
        value = value.ToString().Trim();
        if ((string)value == "") value = null;
      }
      _cmd.Parameters[key].Value = (value == null || value == DBNull.Value) ? DBNull.Value : 
        (_cmd.Parameters[key].SqlDbType == SqlDbType.UniqueIdentifier) ? new Guid(value.ToString()) : value;
    }
  }

  public System.Xml.XmlReader ParmAsXmlReader(string parmName)
  {
    return ((System.Data.SqlTypes.SqlXml)Parms[parmName].SqlValue).CreateReader();
  }

  public virtual void ClearParms()
  {
    for (var i = 0; i < Parms.Count; i++)
      Parms[i].Value = DBNull.Value;
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
public static class SqlClientHelpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
{
  static public long? AsLong(this SqlParameter parm)
  {
    long result;
    return long.TryParse(parm.Value.ToString(), out result) ? result : (long?)null;
  }

  static public string AsString(this SqlParameter parm)
  {
    var result = parm.Value.ToString().Trim();
    return string.IsNullOrWhiteSpace(result) ? null : result;
  }


  /// <summary>
  /// Add to an existing NameValueCollection
  /// </summary>
  static public NameValueCollection DataTableToNameValueCollection(DataTable table, NameValueCollection vals = null)
  {
    if ((table == null) || (table.Rows.Count == 0)) return (null);

    if (table.Columns[0].ColumnName.ToLower() == "name") //row based name-value pairs
    {
      if (vals == null) vals = new NameValueCollection(table.Rows.Count - 1);
      foreach (DataRow row in table.Rows) vals[row["name"].ToString()] = row["value"].ToString();
    }
    else //column based...
    {
      if (vals == null) vals = new NameValueCollection(table.Columns.Count - 1);
      foreach (DataColumn col in table.Columns) vals[col.ColumnName] = table.Rows[0][col.ColumnName].ToString();
    }

    return (vals);
  }

  static public string BuildRowFilter(params string[] filters)
  {
    return (String.Join(" AND ", filters.Where(s => !String.IsNullOrWhiteSpace(s))));
  }

  static public void SetReadonlyField(this DataRow row, string colName, object data)
  {
    bool current = row.Table.Columns[colName].ReadOnly;
    row.Table.Columns[colName].ReadOnly = false;
    row[colName] = data;
    row.Table.Columns[colName].ReadOnly = current;
  }


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //DataRow/View extension methods - use this approach to create a wrapper around DataRowView
  //such that callers stay obvlivious to DataRowView specifics and therefore could be implemented by some other model field container w/o lots of rippling changes

  //static private FieldInfo DataView_OnListChanged = typeof(DataView).GetField("onListChanged", BindingFlags.NonPublic | BindingFlags.Instance);
  //static public void InvokeListChanged(this DataView v)
  //{
  //  ListChangedEventHandler OnListChanged = DataView_OnListChanged.GetValue(v) as ListChangedEventHandler;
  //  if (OnListChanged != null) OnListChanged(v, new ListChangedEventArgs(ListChangedType.Reset, 0));
  //}

  static public T Field<T>(this DataRowView drv, string fieldName)
  {
    return ((drv == null || drv[fieldName] is DBNull || drv.Row.RowState == DataRowState.Detached) ? default(T) : drv.Row.Field<T>(fieldName));
  }


  static public bool IsFieldsModified(this DataRow row, params string[] fieldNames)
  {
    return fieldNames.Any(fieldName => row[fieldName, DataRowVersion.Original].ToString() != row[fieldName, DataRowVersion.Current].ToString());
  }

  static public void SetAllNonDBNullColumnsToEmptyString(this DataRow r, string emptyPlaceholder = "")
  {
    if (r == null) return;
    foreach (DataColumn c in from DataColumn c in r.Table.Columns where !c.AllowDBNull && r[c.ColumnName] == DBNull.Value select c)
      r[c.ColumnName] = emptyPlaceholder;
  }

  /// <summary>
  /// This is the 'undo' of SetAllNonDBNullColumnsToEmptyString()
  /// </summary>
  /// <param name="r"></param>
  /// <param name="emptyPlaceholder"></param>
  static public void RemoveEmptyPlaceholder(this DataRow r, string emptyPlaceholder)
  {
    if (r == null) return;
    foreach (DataColumn c in from DataColumn c in r.Table.Columns where !c.AllowDBNull && r[c.ColumnName].ToString() == emptyPlaceholder select c)
      r[c.ColumnName] = DBNull.Value;
  }


  static private readonly FieldInfo DataRowViewOnPropertyChanged = typeof(DataRowView).GetField("onPropertyChanged", BindingFlags.NonPublic | BindingFlags.Instance);
  static public void SetColumnError(this DataRowView drv, string fieldName, string message)
  {
    drv.Row.SetColumnError(fieldName, message);
    var propertyChangedEventHandler = DataRowViewOnPropertyChanged.GetValue(drv) as PropertyChangedEventHandler;
    if (propertyChangedEventHandler != null)
      propertyChangedEventHandler(drv, new PropertyChangedEventArgs(fieldName));
  }

  static public void ClearColumnError(this DataRowView drv, string fieldName)
  {
    //if no current error, clear previous one (if there was one) and notify UI
    //... trying to minimize notifications as much as possible, rather than just firing notify either way
    if (drv.Row.GetColumnError(fieldName) != "")
    {
      drv.Row.SetColumnError(fieldName, "");
      var propertyChangedEventHandler = DataRowViewOnPropertyChanged.GetValue(drv) as PropertyChangedEventHandler;
      if (propertyChangedEventHandler != null)
        propertyChangedEventHandler(drv, new PropertyChangedEventArgs(fieldName));
    }
  }

  
  public static bool ColumnsContain(this DataRowView drv, string columnName)
  {
    return(drv.Row.Table.Columns.Contains(columnName));
  }

  public static void AcceptChanges(this DataRowView drv)
  {
    drv.Row.AcceptChanges();
  }

  public static void AcceptChanges(this DataView v)
  {
    foreach(DataRowView r in v) r.Row.AcceptChanges();
  }

  public static bool IsDirty(this DataRowView drv)
  {
    return (drv != null && !(drv.Row.RowState == DataRowState.Unchanged || drv.Row.RowState == DataRowState.Detached));
  }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="v"></param>
  /// <param name="respectRowFilter">If true, does NOT clear RowFilter prior to dirty check.</param>
  /// <returns></returns>
  public static bool IsDirty(this DataView v, bool respectRowFilter = true)
  {
    if (v == null) return (false);

    //nugget: this approach of clearing the rowfilter, and then restoring it created a buggy.
    //        TextBox bound to this view would lose it's focus as you were entering text, not gonna work.
    //        the entry would fire the PropertyChanged -> IsModified -> IsDirty()
    //
    //        but the typical save logic really needs a way to know *everything* that should be saved, including Filtered rows
    //        so unfortunately this is a sharp edge that we have to keep around...
    //        namely, IsDirty() calls has been removed from the IsModified logic, IsModified is now a simple get/set property
    //        and IsDirty() should only be called for saves where the user has already changed focus to a Save button anyway

    //Debug.Assert(v.RowFilter == "" || RespectRowFilter == true, "Checking dirty on a DataView with a RowFilter is debatable");

    string saveFilter = "";
    try
    {
      if (!respectRowFilter & v.RowFilter != "")
      {
        saveFilter = v.RowFilter;
        v.RowFilter = "";
      }
      return (v.Cast<DataRowView>().Any(r => r.IsDirty()));
    }
    finally
    {
      if (saveFilter != "") v.RowFilter = saveFilter;
    }
  }

  static public void DetachRowsAndDispose(this DataView v, bool preserveRowFilter = false)
  {
    if (v == null) return;
    if (!preserveRowFilter) v.RowFilter = "";
    foreach (DataRowView r in v) r.DetachRow();
    v.Dispose();
  }

  static public void DetachRow(this DataRowView v)
  {
    if (v == null) return;
    if (v.Row.RowState != DataRowState.Detached)
    {
      v.Row.CancelEdit();
      v.Row.RejectChanges();
      if (v.Row.RowState != DataRowState.Detached)//i guess this happens when we reject a recently added row
        v.Row.Table.Rows.Remove(v.Row);
    }
  }


  static public void AddRelation(this DataSet ds, string name, DataColumn parent, DataColumn child, bool enforceConstraints = true)
  {
    if (!ds.Relations.Contains(name))
    {
      ds.Relations.Add(name, parent, child, enforceConstraints);
    }
  }

  static public void AddColumn(this DataTable table, string colName, Type type, string expression)
  {
    if (!table.Columns.Contains(colName))
      table.Columns.Add(colName, type, expression);
  }

  static public IEnumerable<Dictionary<string, object>> ToSimpleTable(this DataTable t)
  {
      return t.AsEnumerable().Select(r => t.Columns.Cast<DataColumn>().Select(c =>
                           new { Column = c.ColumnName, Value = r[c] })
                       .ToDictionary(i => i.Column, i => i.Value != DBNull.Value ? i.Value : null));
  }

  static public DataRow Clone(this DataRow row)
  {
    return(row.Table.Rows.Add(row.ItemArray));
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  private static readonly string[] SqlErrorTranslations = new[]
  {
    //Cannot insert the value NULL into column 'CreateDate', table 'iTRAACv2.dbo.ClientPreviousSponsor'; column does not allow nulls. INSERT fails.
    @"Cannot insert the value NULL into column '(.+?)', table '(.+?)'",
      "Please fill out the {1} field ({2}).",
    @"The DELETE statement conflicted with the REFERENCE constraint.*table \""(?<object>.+?)\""",
      "You must remove the associated {1}s before you can delete this.",
    @"Cannot insert duplicate key row in object '(?<object>.+?)' with unique index '(.+?)'",
      "{1} already exists (key: {2})", //the assumption here is that unique indexes are named with an eye towards displaying to user when violated
    @"Violation of PRIMARY KEY constraint '(.+?)'. Cannot insert duplicate key in object '(?<object>.+?)'. The duplicate key value is \((.+?)\).",
      "{3} already exists (key: {1}, value: {2})"
  };

  /// <summary>
  /// Regular expression string for patterns to be deleted from designated SQL object name
  /// e.g. "(^tbl|^vw_|^dbo.)"
  /// </summary>
  public static string SqlErrorObjectRemoveRegex //
  {
    get
    {
      return (_sqlErrorObjectRemoveRegex.ToString());
    }
    set
    {
      _sqlErrorObjectRemoveRegex = new Regex(value, RegexOptions.Compiled);
    }
  }
  private static Regex _sqlErrorObjectRemoveRegex;

  static SqlClientHelpers()
  {
    SqlErrorObjectRemoveRegex = @"^dbo\.";
  }

  /// <summary>
  /// Formats raw SQL error text into more human readable wording
  /// e.g. given SQLError_ObjectRemoveRegex = "(^dbo.|^tbl|^vw_|es$|s$)", 
  ///   then "Violation of PRIMARY KEY constraint 'PK1_1'. Cannot insert duplicate key in object 'dbo.tblSponsors'. The duplicate key value is (370000683).\r\nThe statement has been terminated."
  ///   is translated to: "Sponsor already exists (key: PK1_1, value: 370000683)"
  /// </summary>
  /// <param name="message"></param>
  /// <returns></returns>
  public static string SqlErrorTextCleaner(string message)
  {
    for (int i = 0; i < SqlErrorTranslations.Length-1; i += 2)
    {
      var regex = new Regex(SqlErrorTranslations[i]);
      var m = regex.Match(message);
      if (m.Success)
      {
        if (m.Groups["object"].Success)
        {
          string objectname = m.Groups["object"].Value;

          if (_sqlErrorObjectRemoveRegex != null)
          {
            objectname = _sqlErrorObjectRemoveRegex.Replace(objectname, ""); //for example, first time removes "^dbo." 
            objectname = _sqlErrorObjectRemoveRegex.Replace(objectname, ""); //and then second time removes "^tbl"
            //this two step allows the replacement patterns to be prefix targeted rather than openly replacing the pattern found anywhere in the string
          }

          m = regex.Match(message.Replace(m.Groups["object"].Value, objectname)); //repopulate the groups with the updated <object>
        }
        return (String.Format(SqlErrorTranslations[i + 1], m.Groups[0], m.Groups[1], m.Groups[2], m.Groups[3], m.Groups[4], m.Groups[5]));
      }
    }
    return (message);
  }

  public static void AddInitialEmptyRows(DataSet ds, StringCollection rootTables)
  {
    //if root table is totally empty, create an initial row so that the grids show up and are ready to add the first entry
    foreach (var r in rootTables)
    {
      var t = ds.Tables[r];
      if (t.Rows.Count == 0) AddNewRowWithPK(t);
    }

    //now walk the relationships and create initial CHILD rows where necessary
    foreach (DataRelation rel in ds.Relations)
    {
      foreach (DataRow parentRow in rel.ParentTable.Rows)
      {
        if (parentRow.GetChildRows(rel).Length != 0) continue;
        var childRow = AddNewRowWithPK(rel.ChildTable);
        //fill out the foreign-key
        childRow[rel.ChildKeyConstraint.Columns[0].ColumnName] = parentRow[rel.ChildKeyConstraint.RelatedColumns[0].ColumnName];
      }
    }

  }

  public static DataRow AddNewRowWithPK(DataTable t)
  {
    DataRow r = t.NewRow();
    r[t.PrimaryKey[0].ColumnName] = Guid.NewGuid();
    t.Rows.Add(r);
    return (r);
  }

  public static DataRow AddNewNestedRow(DataTable t)
  {
    //create new row, assign PK
    var r = AddNewRowWithPK(t);

    //fill this new row's foreign keys to its parents
    foreach (var col in from DataRelation rel in t.ParentRelations select rel.ChildColumns[0].ColumnName)
    {
      r[col] = t.Rows[0][col];
    }

    //create new empty child rows with their FK's pointing to this new row so any related sub grids display
    foreach (DataRelation rel in t.ChildRelations)
    {
      var col = rel.ParentColumns[0].ColumnName;
      var childrow = AddNewRowWithPK(rel.ChildTable);
      childrow[col] = r[col];
    }

    return (r);
  }
}

