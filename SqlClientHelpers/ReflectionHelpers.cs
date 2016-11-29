using System.Reflection;
using System.ComponentModel;

// ReSharper disable CheckNamespace
public static class ReflectionHelpers
// ReSharper restore CheckNamespace
{
  public static string CurrentAppName { get { return (Assembly.GetEntryAssembly().GetName().Name); } }


  /// <summary>
  /// 
  /// </summary>
  /// <param name="obj"></param>
  /// <param name="property"> </param>
  /// <param name="value"></param>
  /// <returns>True = propery exists on obj</returns>
  public static bool PropertySetter(object obj, string property, object value)
  {
    if (obj == null) return (false);

    //check for propertyName as a public field first as a handy way to pass in simple objects
    FieldInfo textField = obj.GetType().GetField(property);
    if (textField != null)
    {
      textField.SetValue(obj, value);
      return (true);
    }

    //otherwise check for things as public properties so we can handle "Label" objects
    //using System.ComponentModel routines like GetProperties is reportedly the most performance optimized way of doing reflection:
    //http://stackoverflow.com/questions/238555/how-do-i-get-the-value-of-memberinfo
    PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(obj);//[propertyName];
    PropertyDescriptor prop = properties[property];
    if ( (prop == null) && (property == "Text") ) prop = properties["Content"]; //little switcheroo for WPF purposes

    if (prop != null)
    {
      prop.SetValue(obj, value);
      return (true);
    }

    return (false); //propertyName was not found
  }

}
