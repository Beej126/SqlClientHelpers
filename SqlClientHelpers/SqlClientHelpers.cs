using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// ReSharper disable once CheckNamespace
namespace NextTech.SqlClientHelpers
{
  //make sure to keep this clean of any particular UI assembly dependencies so that it can be
  //reused across ASP.Net, Windows.Forms and WPF projects

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /// <summary>
  ///   Basically a wrapper around SqlCommand.
  ///   One benefit is .Parameters is pre-populated and conveniently exposed as Proc[string] indexer.
  /// </summary>
  // ReSharper disable CheckNamespace
  public class Proc : IDisposable
  // ReSharper restore CheckNamespace
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  {
    public static IDisposable NewWaitObject() => new DummyDisposable();

    private class DummyDisposable : IDisposable
    {
      public void Dispose()
      {
      }
    }

    /// <summary>
    /// Default is 30 secs
    /// </summary>
    public static int CommandTimeoutSeconds = 30;

    /// <summary>
    /// this gets set with SqlCommand.Connection.SQLWarnings
    /// mostly for debugging
    /// </summary>
    public string SQLWarnings { get; private set; }

    /// <summary>
    /// Establishing convention that all Sql Executions will trap exceptions and instead fire this event handler
    /// Facilitates typical UI messaging
    /// </summary>
    public Action<Exception> OnError;

    /// <summary>
    /// Static called if not set at proc instance level
    /// </summary>
    public static Action<Exception> OnErrorDefault;

    /// <summary>
    /// Upon successful Sql execution
    /// Facilitates typical UI messaging
    /// </summary>
    public Action OnSuccess;

    /// <summary>
    /// Static called if not set at proc instance level
    /// </summary>
    public static Action OnSuccessDefault;

    /// <summary>
    /// Either a Connection String KEY present in your app/web.config, or a full Connection String
    /// </summary>
    public static string ConnectionStringDefault;

    protected string ConnString;

    public bool TrimAndNull = true;
    private SqlCommand _cmd;
    private readonly string _procName;
    //private readonly string _userName;
    private DataSet _ds;

    private bool _procOwnsDataSet = true;

    public DataSet DataSet
    {
      get { return (_ds); }
      set
      {
        _ds = value;
        _procOwnsDataSet = false;
      }
    }

    /// <summary>
    /// </summary>
    /// <param name="procName"></param>
    /// <param name="connectionString">Pass either full connectiong string or named connection string from .config</param>
    /* <param name="userName">pass this in to set "Workstation ID" which can be obtained via T-SQL's HOST_NAME() function... as a handy layup for a simple table audit framework :)</param>
    // I'm not entirely convinced this is the most elegant way to support this versus something more "automatic" in the background
    // the main challenge is maintaining a generically reusable Proc class that doesn't know whether it's running under ASP.Net or WPF
    // so rather than implementing a bunch of dynamic "drilling" to identify where you are and who the current username is
    // i'm thinking this is a nice "good enough" for now to simply pass it in from the outercontext*/
    public Proc(string procName, string connectionString = null)
    {
      try
      {
        ConnString = connectionString ?? ConnectionStringDefault;
        var configConn = ConfigurationManager.ConnectionStrings[ConnString];
        if (configConn != null) ConnString = configConn.ConnectionString;

        CCrossThrowIf.ThrowIf.Argument.IsNull(() => ConnString,
          "Either Proc.ConnectionStringDefault must be set or specified in Proc constructor");

        _procName = procName;
        if (!_procName.Contains('.')) _procName = "dbo." + _procName;

        //_userName = userName;
        //scratch that idea... see "NOOP" comment further down: if (_ProcName == "NOOP") return; //this supports doing nothing if we're chaining AssignParms(DataRowView).ExecuteNonQuery() and the DataRowView.Row.RowState == UnChanged

        PopulateParameterCollection();
      }
      catch (Exception ex)
      {
        (OnErrorDefault ?? OnError)?.Invoke(ex);
      }
    }

    static Proc()
    {
      ResetParmcache();
    }

    private static Dictionary<string, SqlCommand> _parmcache;

    public static void ResetParmcache()
    {
      _parmcache = new Dictionary<string, SqlCommand>(StringComparer.OrdinalIgnoreCase);
    }

    public SqlParameterCollection Parms => (_cmd.Parameters);

    public string[] TableNames
    {
      get
      {
        CCrossThrowIf.ThrowIf.Argument.IsNull(() => DataSet, $"Proc.TableNames pulled prior to populating dataset (procname: {_procName})");
        return (DataSet.Tables.Cast<DataTable>().Select(t => t.TableName).ToArray());
      }
    }

    public string[] MatchingTableNames(params string[] baseTableNames)
    {
      if (baseTableNames.Length == 0) return (TableNames); //if no table name filter specified, then we want all of them
      return
        (TableNames.Where(
          have => baseTableNames.Any(want => //nugget: compare two lists with linq and take the intersection, 
              Regex.IsMatch(have, want + @"(\.[0-9]+)?$", RegexOptions.IgnoreCase))).ToArray());
      //nugget: with a Regex for the comparison <nice>

      /* this was a fun learning exercise, but there's too much ambiguity involved with IEqualityComparer's dependence on having a rational GetHashCode() available
       * see this: http://stackoverflow.com/questions/98033/wrap-a-delegate-in-an-iequalitycomparer
       * so, sticking with the nested Linq approach above, works just fine and there's no screwy edge cases to worry about

       OnlyTableNames = OnlyTableNames.Intersect(proc.TableNames, //for an even more Linq'y approach, use built in .Intersect() ... 
          new LambdaComparer<string>((have, want) => //but we need a helper class to wrapper the required IEqualityComparer<T> ...
            Regex.IsMatch(have, want + @"(\.[0-9]+)?$", RegexOptions.IgnoreCase))).ToArray(); //around a comparison lambda */
    }

    private static readonly Regex UdtParamTypeNameFix = new Regex(@"(.*?)\.(.*?)\.(.*)", RegexOptions.Compiled);

    private void PopulateParameterCollection(bool refreshCache = false)
    {
      //pull cached parms if available
      var logicalConnectionString = ConnString; //+ ((_userName != null) ? ";Workstation ID=" + _userName : "");
      var parmcachekey = logicalConnectionString + "~" + _procName;

      //this is used to facilitate an automatic parm load retry when we run into a "parameter missing" exception
      if (refreshCache)
      {
        _parmcache.Remove(parmcachekey);
        return;
      }

      var hasCachedParms = _parmcache.TryGetValue(parmcachekey, out _cmd);
      _cmd = hasCachedParms ? _cmd.Clone() : new SqlCommand(_procName) { CommandType = CommandType.StoredProcedure };
      _cmd.CommandTimeout = CommandTimeoutSeconds;
      _cmd.Connection = new SqlConnection(logicalConnectionString);
      if (_cmd.Connection.State != ConnectionState.Open) _cmd.Connection.Open();

      if (hasCachedParms) return;

      //i love this little gem...
      //this allows us to skip all that noisy boiler plate proc parm definition code in the calling context, and simply assign parm names to values 
      //nugget: automatically assigns all the available parms to this SqlCommand object by querying SQL Server's proc definition metadata
      SqlCommandBuilder.DeriveParameters(_cmd);

      //strip the dbname off any UDT's... there appears to be a mismatch between the part of microsoft that wrote DeriveParameters and what SQL Server actually wants
      //otherwise you get this friendly error message:
      //The incoming tabular data stream (TDS) remote procedure call (RPC) protocol stream is incorrect. Table-valued parameter 1 ("@MyTable"), row 0, column 0: Data type 0xF3 (user-defined table type) has a non-zero length database name specified.  Database name is not allowed with a table-valued parameter, only schema name and type name are valid.
      foreach (SqlParameter p in _cmd.Parameters)
        if (p.TypeName != "")
        {
          var m = UdtParamTypeNameFix.Match(p.TypeName);
          if (m.Success) p.TypeName = m.Groups[2] + "." + m.Groups[3];
        }

      //nugget: cache SqlCommand objects to avoid unnecessary SqlCommandBuilder.DeriveParameters() calls
      _parmcache.Add(parmcachekey, _cmd.Clone());
    }

    public Proc AssignParms(Dictionary<string, object> values)
    {
      foreach (var nv in values) this["@" + nv.Key] = nv.Value;
      return this;
    }

    public Proc AssignParms(NameValueCollection values)
    {
      foreach (string key in values.Keys) this["@" + key] = values[key];
      return this;
    }

    public Proc AssignParms(DataRowView values)
    {
      return (AssignParms(values.Row));
    }

    public Proc AssignParms(DataRow values)
    //nugget: giving the "fluent" API approach a shot: http://en.wikipedia.org/wiki/Fluent_interface, well ok, starting with simple method chaining then: http://martinfowler.com/bliki/FluentInterface.html
    {
      //don't do this, then we can't chain multiple things together where one may be dirty at the end!! if (values.Row.RowState == DataRowState.Unchanged) return new Proc("NOOP");

      foreach (DataColumn col in values.Table.Columns)
      {
        var colname = "@" + col.ColumnName;

        if (!_cmd.Parameters.Contains(colname))
          colname = "@" + col.ColumnName.Replace(" ", ""); //try mapping to columns by removing spaces in the name

        this[colname] = values[col.ColumnName];
      }
      return this;
    }


    public Proc AssignParms(object[] values)
    {
      for (var i = 0; i < values.Length; i += 2) this["@" + values[i]] = values[i + 1];
      return this;
    }

    public void Dispose()
    {
      if (_cmd != null)
      {
        _cmd.Connection.InfoMessage -= ConnectionInfoMessageHandler;
        _cmd.Connection.Dispose();
        _cmd.Connection = null;
        _cmd.Dispose();
        _cmd = null;
      }

      if (_ds != null && _procOwnsDataSet) _ds.Dispose();
      _ds = null;
    }

    public DataSet ExecuteDataSet()
    {
      using (NewWaitObject())
      using (var da = new SqlDataAdapter(_cmd))
      {
        if (_ds == null) _ds = new DataSet();
        _ds.EnforceConstraints = false;
        if (_cmd.Connection.State != ConnectionState.Open) _cmd.Connection.Open();
        _cmd.Connection.InfoMessage += ConnectionInfoMessageHandler; //nugget: capture print/raiserror<11 output

        //AddWithKey tells ADO.Net to bring back the schema info like DataColumn.MaxLength (otherwise they're populated with nonsense values)
        da.MissingSchemaAction = MissingSchemaAction.AddWithKey;

        try
        {
          da.Fill(_ds);
          NameTables();
          (OnSuccessDefault ?? OnSuccess)?.Invoke();
        }
        catch (Exception ex)
        {
          //nugget: auto try reloading parms for the scenario where parms are cached and a new parameter has since been added
          if (ex.Message.Contains("expects parameter"))
          {
            PopulateParameterCollection(refreshCache: true);
            try
            {
              da.Fill(_ds);
              NameTables();
              (OnSuccessDefault ?? OnSuccess)?.Invoke();
            }
            catch (Exception ex2)
            {
              (OnErrorDefault ?? OnError)?.Invoke(ex2);
            }
          }
          else (OnErrorDefault ?? OnError)?.Invoke(ex);
        }
      }

      //turn off the Readonly flag to allow for what i consider the typical scenario of modifying values on client, and re-submitting changes back to server
      foreach (DataTable table in _ds.Tables) foreach (DataColumn column in table.Columns) column.ReadOnly = false;

      return (_ds);
    }

    public async Task<DataSet> ExecuteDataSetAsync() => await Task.Run(() => ExecuteDataSet());

    private void ConnectionInfoMessageHandler(object sender, SqlInfoMessageEventArgs e)
    {
      SQLWarnings = e.Message;
    }

    private void NameTables()
    {
      //establish a nice little convention here... sprocs can return a list of "friendly" names for each resultset...
      //(not required, just supported if the @TableNames output parm is present)
      //so, if the proc is telling us it's list of tables names, rename the generic Table0, Table1, etc to the corresponding names so caller can reference them more explicitly
      if (_cmd.Parameters.Contains("@TableNames"))
      {
        var tableNames = this["@TableNames"].ToString().ToLower().CleanCommas().Split(',')
          //we'd like the flexibility to send back multiple resultsets with the *SAME* name
          //this allows us the non trivial freedom to fire nested proc calls which return data headed into the same table without coupling those procs' internal logic with the need to do a union
          //but DataSet doesn't allow duplicate table names
          //so, .Uniquify() loops through the strings and renames them to table.1, table.2 etc
          //(the client will just have to be involved with anticipating this and consuming appropriately)
          .Uniquify().ToArray();

        Debug.Assert(tableNames.Length == _ds.Tables.Count,
          $"{_procName}[@TableNames] specifies {tableNames.Length} table(s), but proc returned {_ds.Tables.Count} resultsets.");

        //at this point it's just a matter of renaming the DataSet.Tables by ordinal mapping to our unique list of tablenames...
        for (var i = 0; i < tableNames.Length; i++) _ds.Tables[i].TableName = tableNames[i];

        //and lastly, sync the @TableNames parm back up with whatever renaming we've done
        //just in case the caller depends on this being consistent, but preferrably callers will rely on the Proc.TableNames property for this
        this["@TableNames"] = string.Join(",", TableNames);
      }
    }

    /// <summary>
    /// remember to wrap in using{} or otherwise call .Dispose
    /// </summary>
    public async Task<SqlDataReader> ExecuteReaderAsync()
    {
      try
      {
        var reader = await _cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);
        (OnSuccessDefault ?? OnSuccess)?.Invoke();
        return reader;
      }
      catch (Exception ex)
      {
        (OnErrorDefault ?? OnError)?.Invoke(ex);
        return null;
      }
    }

    /// <summary>
    /// Skips overhead of capturing results into Dataset.
    /// Useful for Sproc Parameter only executions - noteably: output parms are populated
    /// </summary>
    /// <returns>True = executed without exception</returns>
    public async Task<bool> ExecuteNonQueryAsync()
    {
      if (_cmd == null) return false;

      using (NewWaitObject())
      {
        try
        {
          await _cmd.ExecuteNonQueryAsync();
          (OnSuccessDefault ?? OnSuccess)?.Invoke();
          return true;
        }
        catch (Exception ex)
        {
          //nugget: auto try reloading parms for the scenario where parms are cached and a new parameter has since been added
          if (ex.Message.Contains("expects parameter"))
          {
            PopulateParameterCollection(refreshCache: true);
            try
            {
              await _cmd.ExecuteNonQueryAsync();
              (OnSuccessDefault ?? OnSuccess)?.Invoke();
              return true;
            }
            catch (Exception ex2)
            {
              (OnErrorDefault ?? OnError)?.Invoke(ex2);
            }
          }
          else (OnErrorDefault ?? OnError)?.Invoke(ex);
        }
        return false;
      }
    }

    public T GetParmAs<T>(string key)
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

        throw new StrongTypingException("Cannot get strong typed value since it is DB null. Please use a nullable type.",
          null);
      }

      //http://stackoverflow.com/questions/2961656/generic-tryparse
      try
      {
        return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFrom(ret);
      }
      catch
      {
        return default(T);
      }
    }

    /// <summary>
    /// Sproc Parameters
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public object this[string key]
    {
      //nulls can get a little tricky to look at here...
      //if TrimAndNull is on, then it'll truncate a blank string and convert it to DBNull

      //this getter here returns SqlValue not "Value" ... which translates SqlString parameters containing DBNull into C# nulls... 
      //this may or may not come in handy... we'll have to see and tweak accordingly
      get { return (_cmd.Parameters[key]?.Value); }
      set
      {
        if (!_cmd.Parameters.Contains(key)) return; //silently skip nonexisting parm names

        if (TrimAndNull && (value != null) && (value != DBNull.Value) &&
            (value is string || value is SqlChars || value is SqlString))
        {
          value = value.ToString().Trim();
          if ((string)value == "") value = null;
        }
        _cmd.Parameters[key].Value = (value == null || value == DBNull.Value)
          ? DBNull.Value
          : (_cmd.Parameters[key].SqlDbType == SqlDbType.UniqueIdentifier) ? new Guid(value.ToString()) : value;
      }
    }

    public virtual void ClearParms()
    {
      for (var i = 0; i < Parms.Count; i++)
        Parms[i].Value = DBNull.Value;
    }
  }


  /*TODO: just haven't seen a performance driver for a lighter implementation than the ADO.Net objects
  public class SimpleTable
  {
    public Dictionary<string, int> ColumnNames;
    public SimpleRecord[] Rows;
    private object[][] _rawData;

    public SimpleTable(SqlDataReader reader)
    {
      while (reader.Read())
      {
        reader.GetValues()
      }
    }

    public class SimpleRecord
    {
      private readonly SimpleTable _table;
      private readonly object[] _rawData;
      internal SimpleRecord(SimpleTable table, object[] rawData)
      {
        _table = table;
        _rawData = rawData;
      }

      public T GetAs<T>(string fieldName)
      {
        //return (T) TypeDescriptor.GetConverter(typeof(T)).ConvertFromString(ret.ToString());
        return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertFrom(_rawData[_table.ColumnNames[fieldName]]);
      }
    }

  }
  */

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  public static class SqlClientHelpers
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  {
    public static DataTable Table0(this DataSet ds) => ds.Tables.Count > 0 ? ds.Tables[0] : null;

    public static DataRow Row0(this DataSet ds) => ds.Table0()?.Rows.Count > 0 ? ds.Table0()?.Rows[0] : null;


    public static long? AsLong(this SqlParameter parm)
    {
      long result;
      return long.TryParse(parm.Value.ToString(), out result) ? result : (long?)null;
    }

    public static string AsString(this SqlParameter parm)
    {
      var result = parm.Value.ToString().Trim();
      return string.IsNullOrWhiteSpace(result) ? null : result;
    }

    public static IEnumerable<Dictionary<string, object>> ToDictionaryRows(this DataTable t)
    {
      return t.Rows.Cast<DataRow>().Select(r => t.Columns.Cast<DataColumn>().Select(c =>
            new { Column = c.ColumnName, Value = r[c] })
        .ToDictionary(i => i.Column, i => i.Value != DBNull.Value ? i.Value : null));
    }

    //from: http://stackoverflow.com/a/34927336/813599
    //with edits: removed naming each table
    //TODO: this could however be extended to use @TableNames output parm convention elsewhere in this code if that proved useful
    public static async Task ToJsonAsync(this SqlDataReader reader, Stream stream)
    {
      if (reader == null) return;
      using (reader)
      using (var writer = new JsonTextWriter(new StreamWriter(stream)))
      {
        writer.WriteStartArray();
        do
        {
          writer.WriteStartArray();
          while (await reader.ReadAsync())
          {
            writer.WriteStartObject();
            for (var columnIndex = 0; columnIndex < reader.FieldCount; columnIndex++)
            {
              if (reader.IsDBNull(columnIndex)) continue;
              writer.WritePropertyName(reader.GetName(columnIndex));
              writer.WriteValue(reader.GetValue(columnIndex));
            }
            writer.WriteEndObject();
          }
          writer.WriteEndArray();
        } while (await reader.NextResultAsync());

        writer.WriteEndArray();
        writer.Flush();
      }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="table"></param>
    /// <param name="vals">add to an existing collection instance</param>
    /// <returns></returns>
    public static NameValueCollection ToNameValueCollection(this DataTable table, NameValueCollection vals = null)
    {
      if ((table == null) || (table.Rows.Count == 0)) return (null);

      if (table.Columns[0].ColumnName.ToLower() == "name") //row based name-value pairs
      {
        if (vals == null) vals = new NameValueCollection(table.Rows.Count);
        foreach (DataRow row in table.Rows) vals[row["name"].ToString()] = row["value"].ToString();
      }
      else //column based...
      {
        if (vals == null) vals = new NameValueCollection(table.Columns.Count - 1);
        foreach (DataColumn col in table.Columns) vals[col.ColumnName] = table.Rows[0][col.ColumnName].ToString();
      }

      return (vals);
    }

    public static string BuildRowFilter(params string[] filters)
    {
      return (string.Join(" AND ", filters.Where(s => !string.IsNullOrWhiteSpace(s))));
    }

    public static void SetReadonlyField(this DataRow row, string colName, object data)
    {
      var current = row.Table.Columns[colName].ReadOnly;
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

    public static T Field<T>(this DataRowView drv, string fieldName)
    {
      return ((drv == null || drv[fieldName] is DBNull || drv.Row.RowState == DataRowState.Detached)
        ? default(T)
        : drv.Row.Field<T>(fieldName));
    }


    public static bool IsFieldsModified(this DataRow row, params string[] fieldNames)
    {
      return
        fieldNames.Any(
          fieldName =>
              row[fieldName, DataRowVersion.Original].ToString() != row[fieldName, DataRowVersion.Current].ToString());
    }

    public static void SetAllNonDbNullColumnsToEmptyString(this DataRow r, string emptyPlaceholder = "")
    {
      if (r == null) return;
      foreach (
        var c in from DataColumn c in r.Table.Columns where !c.AllowDBNull && r[c.ColumnName] == DBNull.Value select c)
        r[c.ColumnName] = emptyPlaceholder;
    }

    /// <summary>
    ///   This is the 'undo' of SetAllNonDBNullColumnsToEmptyString()
    /// </summary>
    /// <param name="r"></param>
    /// <param name="emptyPlaceholder"></param>
    public static void RemoveEmptyPlaceholder(this DataRow r, string emptyPlaceholder)
    {
      if (r == null) return;
      foreach (
        var c in
        from DataColumn c in r.Table.Columns
        where !c.AllowDBNull && r[c.ColumnName].ToString() == emptyPlaceholder
        select c)
        r[c.ColumnName] = DBNull.Value;
    }


    private static readonly FieldInfo DataRowViewOnPropertyChanged = typeof(DataRowView).GetField("onPropertyChanged",
      BindingFlags.NonPublic | BindingFlags.Instance);

    public static void SetColumnError(this DataRowView drv, string fieldName, string message)
    {
      drv.Row.SetColumnError(fieldName, message);
      var propertyChangedEventHandler = DataRowViewOnPropertyChanged.GetValue(drv) as PropertyChangedEventHandler;
      propertyChangedEventHandler?.Invoke(drv, new PropertyChangedEventArgs(fieldName));
    }

    public static void ClearColumnError(this DataRowView drv, string fieldName)
    {
      //if no current error, clear previous one (if there was one) and notify UI
      //... trying to minimize notifications as much as possible, rather than just firing notify either way
      if (drv.Row.GetColumnError(fieldName) == "") return;
      drv.Row.SetColumnError(fieldName, "");
      var propertyChangedEventHandler = DataRowViewOnPropertyChanged.GetValue(drv) as PropertyChangedEventHandler;
      propertyChangedEventHandler?.Invoke(drv, new PropertyChangedEventArgs(fieldName));
    }


    public static bool ColumnsContain(this DataRowView drv, string columnName)
    {
      return (drv.Row.Table.Columns.Contains(columnName));
    }

    public static void AcceptChanges(this DataRowView drv)
    {
      drv.Row.AcceptChanges();
    }

    public static void AcceptChanges(this DataView v)
    {
      foreach (DataRowView r in v) r.Row.AcceptChanges();
    }

    public static bool IsDirty(this DataRowView drv)
    {
      return (drv != null && !(drv.Row.RowState == DataRowState.Unchanged || drv.Row.RowState == DataRowState.Detached));
    }

    /// <summary>
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

      var saveFilter = "";
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

    public static void DetachRowsAndDispose(this DataView v, bool preserveRowFilter = false)
    {
      if (v == null) return;
      if (!preserveRowFilter) v.RowFilter = "";
      foreach (DataRowView r in v) r.DetachRow();
      v.Dispose();
    }

    public static void DetachRow(this DataRowView v)
    {
      if (v == null) return;
      if (v.Row.RowState != DataRowState.Detached)
      {
        v.Row.CancelEdit();
        v.Row.RejectChanges();
        if (v.Row.RowState != DataRowState.Detached) //i guess this happens when we reject a recently added row
          v.Row.Table.Rows.Remove(v.Row);
      }
    }


    public static void AddRelation(this DataSet ds, string name, DataColumn parent, DataColumn child,
      bool enforceConstraints = true)
    {
      if (!ds.Relations.Contains(name))
      {
        ds.Relations.Add(name, parent, child, enforceConstraints);
      }
    }

    public static void AddColumn(this DataTable table, string colName, Type type, string expression)
    {
      if (!table.Columns.Contains(colName))
        table.Columns.Add(colName, type, expression);
    }

    public static DataRow Clone(this DataRow row)
    {
      return (row.Table.Rows.Add(row.ItemArray));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private static readonly string[] SqlErrorTranslations =
    {
    //Cannot insert the value NULL into column 'CreateDate', table 'iTRAACv2.dbo.ClientPreviousSponsor'; column does not allow nulls. INSERT fails.
    @"Cannot insert the value NULL into column '(.+?)', table '(.+?)'",
    "Please fill out the {1} field ({2}).",
    @"The DELETE statement conflicted with the REFERENCE constraint.*table \""(?<object>.+?)\""",
    "You must remove the associated {1}s before you can delete this.",
    @"Cannot insert duplicate key row in object '(?<object>.+?)' with unique index '(.+?)'",
    "{1} already exists (key: {2})",
    //the assumption here is that unique indexes are named with an eye towards displaying to user when violated
    @"Violation of PRIMARY KEY constraint '(.+?)'. Cannot insert duplicate key in object '(?<object>.+?)'. The duplicate key value is \((.+?)\).",
    "{3} already exists (key: {1}, value: {2})"
  };

    /// <summary>
    ///   Regular expression string for patterns to be deleted from designated SQL object name
    ///   e.g. "(^tbl|^vw_|^dbo.)"
    /// </summary>
    public static string SqlErrorObjectRemoveRegex //
    {
      get { return (_sqlErrorObjectRemoveRegex.ToString()); }
      set { _sqlErrorObjectRemoveRegex = new Regex(value, RegexOptions.Compiled); }
    }

    private static Regex _sqlErrorObjectRemoveRegex;

    static SqlClientHelpers()
    {
      SqlErrorObjectRemoveRegex = @"^dbo\.";
    }

    /// <summary>
    ///   Formats raw SQL error text into more human readable wording
    ///   e.g. given SQLError_ObjectRemoveRegex = "(^dbo.|^tbl|^vw_|es$|s$)",
    ///   then "Violation of PRIMARY KEY constraint 'PK1_1'. Cannot insert duplicate key in object 'dbo.tblSponsors'. The
    ///   duplicate key value is (370000683).\r\nThe statement has been terminated."
    ///   is translated to: "Sponsor already exists (key: PK1_1, value: 370000683)"
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public static string SqlErrorTextCleaner(string message)
    {
      for (var i = 0; i < SqlErrorTranslations.Length - 1; i += 2)
      {
        var regex = new Regex(SqlErrorTranslations[i]);
        var m = regex.Match(message);
        if (m.Success)
        {
          if (m.Groups["object"].Success)
          {
            var objectname = m.Groups["object"].Value;

            if (_sqlErrorObjectRemoveRegex != null)
            {
              objectname = _sqlErrorObjectRemoveRegex.Replace(objectname, ""); //for example, first time removes "^dbo." 
              objectname = _sqlErrorObjectRemoveRegex.Replace(objectname, ""); //and then second time removes "^tbl"
                                                                               //this two step allows the replacement patterns to be prefix targeted rather than openly replacing the pattern found anywhere in the string
            }

            m = regex.Match(message.Replace(m.Groups["object"].Value, objectname));
            //repopulate the groups with the updated <object>
          }
          return
          (string.Format(SqlErrorTranslations[i + 1], m.Groups[0], m.Groups[1], m.Groups[2], m.Groups[3], m.Groups[4],
            m.Groups[5]));
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
        if (t.Rows.Count == 0) AddNewRowWithPk(t);
      }

      //now walk the relationships and create initial CHILD rows where necessary
      foreach (DataRelation rel in ds.Relations)
      {
        foreach (DataRow parentRow in rel.ParentTable.Rows)
        {
          if (parentRow.GetChildRows(rel).Length != 0) continue;
          var childRow = AddNewRowWithPk(rel.ChildTable);
          //fill out the foreign-key
          childRow[rel.ChildKeyConstraint.Columns[0].ColumnName] =
            parentRow[rel.ChildKeyConstraint.RelatedColumns[0].ColumnName];
        }
      }
    }

    public static DataRow AddNewRowWithPk(DataTable t)
    {
      var r = t.NewRow();
      r[t.PrimaryKey[0].ColumnName] = Guid.NewGuid();
      t.Rows.Add(r);
      return (r);
    }

    public static DataRow AddNewNestedRow(DataTable t)
    {
      //create new row, assign PK
      var r = AddNewRowWithPk(t);

      //fill this new row's foreign keys to its parents
      foreach (var col in from DataRelation rel in t.ParentRelations select rel.ChildColumns[0].ColumnName)
      {
        r[col] = t.Rows[0][col];
      }

      //create new empty child rows with their FK's pointing to this new row so any related sub grids display
      foreach (DataRelation rel in t.ChildRelations)
      {
        var col = rel.ParentColumns[0].ColumnName;
        var childrow = AddNewRowWithPk(rel.ChildTable);
        childrow[col] = r[col];
      }

      return (r);
    }
  }

}
