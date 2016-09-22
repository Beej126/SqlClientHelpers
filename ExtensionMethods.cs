using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Linq;
using System.Runtime.Remoting.Metadata.W3cXsd2001;

//make sure to keep this clean of any particular UI assembly dependencies so that it can be
//reused across ASP.Net, Windows.Forms and WPF projects

// ReSharper disable CheckNamespace
public static class Extensions
// ReSharper restore CheckNamespace
{
  public static T ToEnum<T>(this string value) //nugget: too bad C# doesn't support this constraint - where T : Enum
  {
    return((T)Enum.Parse(typeof(T), value));
  }

  public static T SetFlags<T>(this Enum current, T setflags, bool onoff)
  {
    return (onoff ? (T)(object)(((int)(object)current | (int)(object)setflags)) : (T)(object)(((int)(object)current & ~(int)(object)setflags)));
  }

  public static bool HasAnyFlags<T>(this Enum type, T value)
  {
    try
    {
      return (((int)(object)type & (int)(object)value) > 0);
    }
    catch
    {
      return false;
    }
  }

  /*
  public static bool HasAll<T>(this System.Enum type, T value)
  {
    try
    {
      return (((int)(object)type & (int)(object)value) == (int)(object)value);
    }
    catch
    {
      return false;
    }
  }

  public static bool Is<T>(this System.Enum type, T value)
  {
    try
    {
      return (int)(object)type == (int)(object)value;
    }
    catch
    {
      return false;
    }
  }

  public static T Add<T>(this System.Enum type, T value)
  {
    try
    {
      return (T)(object)(((int)(object)type | (int)(object)value));
    }
    catch (Exception ex)
    {
      throw new ArgumentException(
          string.Format(
              "Could not append value from enumerated type '{0}'.",
              typeof(T).Name
              ), ex);
    }
  }


  public static T Remove<T>(this System.Enum type, T value)
  {
    try
    {
      return (T)(object)(((int)(object)type & ~(int)(object)value));
    }
    catch (Exception ex)
    {
      throw new ArgumentException(
          string.Format(
              "Could not remove value from enumerated type '{0}'.",
              typeof(T).Name
              ), ex);
    }
  }
  */



  public static string CleanCommas(this string s)
  {
    s = Regex.Replace(s, "\\s*,\\s*", ","); //drop spaces between commas
    s = Regex.Replace(s, "(^,)|(,$)", ""); //drop any empty leading or trailing commas
    return(s);
  }

  /// <summary>
  /// rename duplicate strings as .1, .2, etc.
  /// </summary>
  /// <param name="enumerable"></param>
  /// <returns></returns>
  public static IEnumerable<string> Uniquify(this IEnumerable<string> enumerable)
  {
    enumerable = enumerable.Where(s => !String.IsNullOrWhiteSpace(s)); //filter out null or empty strings, this might not be preferred in all contexts but for now i'm going to keep it here rather than higher up
    //apparently we can't have this simple return at the top of an iterator implementation (i.e. using "yield" syntax)
    //so i'm just wrappering it to get around that limitation
    //basically, avoid the work of looping through renaming things if there aren't any duplicates in the list
// ReSharper disable PossibleMultipleEnumeration
    return enumerable.Distinct().Count() == enumerable.Count() ? enumerable : enumerable.UniquifyCore();
// ReSharper restore PossibleMultipleEnumeration
  }

  private static IEnumerable<string> UniquifyCore(this IEnumerable<string> enumerable)
  {
    //my implementation doesn't seem terribly efficient... would love to see something better
    //to articulate the challenge... 
    //  adding postfix to only those items which are duplicates - seems to imply the list must be prescanned (hence the Where clause)
    //  keeping the existing order items - implies a simple interation over each element
    //  incrementing the postfix - implies keeping track of the current count of each item as you go through (hence the Dictionary)
    var sameCount = new Dictionary<string, int>();
    Debug.Assert(enumerable != null, "enumerable != null");
    // ReSharper disable PossibleMultipleEnumeration
    foreach (var name in enumerable)
    {
      if (enumerable.Count(n => n == name) > 1)
      {
        sameCount[name] = sameCount.GetValueOrDefault(name) + 1;
        yield return name + "." + sameCount[name].ToString(CultureInfo.InvariantCulture);
      }
      else
        yield return name;
    }
    // ReSharper restore PossibleMultipleEnumeration
  }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="s"></param>
  /// <param name="count">negative count "slices" characters from the right end (ala javascript)</param>
  /// <returns></returns>
  public static string Right(this string s, int count)
  {
    count = ((count < 0) ? -1 : 1) * Math.Min(s.Length, Math.Abs(count)); //automatically assume max length if specified larger than rational
    return (s.Substring((count < 0) ? 0 : s.Length - count, (count < 0) ? s.Length + count : count));
  }

  /// <summary>
  /// 
  /// </summary>
  /// <param name="s"></param>
  /// <param name="count">negative count "slices" characters from the left end (ala javascript)</param>
  /// <returns></returns>
  public static string Left(this string s, int count)
  {
    count = ((count < 0) ? -1 : 1) * Math.Min(s.Length, Math.Abs(count)); //automatically assume max length if specified larger than rational
    return (s.Substring((count < 0) ? -count : 0, (count < 0) ? s.Length + count : count));
  }

  public static string ToHexString(this byte[] bytes)
  {
    return (new SoapHexBinary(bytes).ToString()); //from here: http://stackoverflow.com/questions/311165/how-do-you-convert-byte-array-to-hexadecimal-string-and-vice-versa-in-c
	}

  public static byte[] HexStringToBytes(this string str)
  {
    return (SoapHexBinary.Parse(str).Value);
  }

  static private readonly Regex PluralizeKeyword = new Regex("{(s|es)}", RegexOptions.IgnoreCase | RegexOptions.Compiled);
  static public string Pluralize(string text, int count, params object[] parms)
  {
    var pluralkey = PluralizeKeyword.Match(text);
    if (text.Contains("{count}")) text = text.Replace("{count}", count.ToString(CultureInfo.InvariantCulture));

    return (String.Format(
        text.Replace(pluralkey.Value, (count > 1) ? pluralkey.Groups[1].Value : ""), parms));
  }

  static private readonly Type[] NumericTypes = new[] { typeof(Byte), typeof(Decimal), typeof(Double),
        typeof(Int16), typeof(Int32), typeof(Int64), typeof(SByte),
        typeof(Single), typeof(UInt16), typeof(UInt32), typeof(UInt64)};

  public static bool IsNumeric(this Type type)
  {
    return !(type == null) && NumericTypes.Contains(type);
  }

  //nugget: from here: http://underground.infovark.com/2008/09/02/converting-ienumerable-to-a-comma-delimited-string/
  public static string Join(this IEnumerable strings, string seperator)  //nugget: this is awesome
  {
    var en = strings.GetEnumerator();
    var sb = new StringBuilder();
    if (en.MoveNext())
    {
      // we have at least one item.
      sb.Append(en.Current);
    }
    while (en.MoveNext())
    {
      // second and subsequent get delimiter
      sb.Append(seperator).Append(en.Current);
    }
    return sb.ToString();
  }

  public static TValue GetValueOrDefault<TKey, TValue> (this IDictionary<TKey, TValue> dictionary, TKey key)
  {
    TValue ret;
    // Ignore return value
    dictionary.TryGetValue(key, out ret);
    return ret;
  }

  public static DateTime MondayOfWeek(this DateTime dt)
  {
    return (dt.AddDays(-1 * (dt.DayOfWeek - DayOfWeek.Monday)));
  }


}
