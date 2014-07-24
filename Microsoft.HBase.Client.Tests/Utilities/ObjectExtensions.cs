// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

namespace Microsoft.HBase.Client.Tests.Utilities
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using Microsoft.HBase.Client.Internal;

    /// <summary>
    /// Extends the object system for testing.
    /// </summary>
    internal static class ObjectExtensions
    {
        private static readonly Type[] ExtendedPrimitives = TestHelp.Array(
            typeof(string), typeof(Guid), typeof(DateTime), typeof(DateTimeOffset), typeof(TimeSpan));

        private static readonly Type[] FloatTypes = TestHelp.Array(typeof(float), typeof(double), typeof(decimal));

        private static readonly Type[] IntegerTypes = TestHelp.Array(
            typeof(bool), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong));

        private static readonly Type[] SignedTypes = TestHelp.Array(typeof(sbyte), typeof(short), typeof(int), typeof(long));
        private static readonly Type[] UnsignedTypes = TestHelp.Array(typeof(bool), typeof(byte), typeof(ushort), typeof(uint), typeof(ulong));
        private static readonly IEnumerable<Type> knownTypes = FindKnownTypes();

        /// <summary>
        /// Creates a new instance of a supplied type.
        /// </summary>
        /// <param name = "type">
        /// The type to create an instance for.
        /// </param>
        /// <param name = "parameters">
        /// The parameters to use when creating the type.
        /// </param>
        /// <returns>
        /// A new instance of the created type.
        /// </returns>
        internal static object Create(this Type type, params object[] parameters)
        {
            const BindingFlags BindingFlags = BindingFlags.CreateInstance | BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;
            return InternalDynaCreate(type, typeof(string), BindingFlags, parameters);
        }

        /// <summary>
        /// An internal Method funneled to by other methods used to perform the underlying 
        /// reflection operation necessary to create a type dynamically given the type object
        /// that represents it.
        /// </summary>
        /// <param name = "concreteType">
        /// The type of object to create.
        /// </param>
        /// <param name = "nullType">
        /// The type to be supplied for any supplied parameter that has a null value.
        /// </param>
        /// <param name = "bindings">
        /// The Binding Flags to use when locating the constructor.
        /// </param>
        /// <param name = "parameters">
        /// The parameters to use when calling the constructor.
        /// </param>
        /// <returns>
        /// A new instance of the requested object or null if it could not be constructed.
        /// </returns>
        private static object InternalDynaCreate(Type concreteType, Type nullType, BindingFlags bindings, object[] parameters)
        {
            // Handel CLRs "implicit constructor" for value types.
            if (concreteType.IsValueType && (parameters.IsNull() || parameters.Length == 0))
            {
                return Activator.CreateInstance(concreteType);
            }

            var bestMatch = LocateBestMatch<ConstructorInfo>(concreteType, null, nullType, bindings, parameters, null);

            if (bestMatch != null)
            {
                return InternalDynaInvoke(bestMatch, parameters);
            }
            else
            {
                string msg = "Unable to locate requested constructor for type {0}".FormatIc(concreteType.Name);
                throw new MissingMethodException(msg);
            }
        }

        /// <summary>
        /// Gets the types that provide extension methods.
        /// </summary>
        internal static IEnumerable<Type> ExtensionTypes { get; private set; }

        /// <summary>
        /// Gets the known types within the system.
        /// </summary>
        internal static IEnumerable<Type> KnownTypes
        {
            get { return knownTypes; }
        }

        /// <summary>
        /// Performs an as operation on the supplied object.
        /// </summary>
        /// <typeparam name="T"> The target type of the as operation. </typeparam>
        /// <param name="inputValue"> The object. </param>
        /// <returns> The result of the as operation. </returns>
        internal static T As<T>(this object inputValue) where T : class
        {
            return inputValue as T;
        }

        /// <summary>
        /// Helper method used to find all known types.
        /// </summary>
        /// <returns>
        /// The known types currently loaded in the system.
        /// </returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "By design since we can ignore missing assemblies.  dps")]
        internal static IEnumerable<Type> FindKnownTypes()
        {
            Assembly asm = Assembly.GetExecutingAssembly();
            foreach (AssemblyName reference in asm.GetReferencedAssemblies())
            {
                Assembly.Load(reference.FullName);
            }

            var foundTypes = new List<Type>();
            var foundExtensions = new List<Type>();
            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                // ReSharper disable EmptyGeneralCatchClause
                try
                {
                    foreach (Type type in assembly.GetTypes())
                    {
                        foundTypes.Add(type);
                        if (type.IsSealed)
                        {
                            if (
                                type.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
                                    .Any(methodInfo => methodInfo.IsDefined(typeof(ExtensionAttribute), true)))
                            {
                                foundExtensions.Add(type);
                            }
                        }
                    }
                }
                catch (ReflectionTypeLoadException rtle)
                {
                    // this can occur because a type in the assembly is decorated with: [StructLayout(LayoutKind.Explicit)]
                    foreach (Type type in rtle.Types)
                    {
                        if (type.IsNull())
                        {
                            continue;
                        }

                        foundTypes.Add(type);
                        if (
                            type.GetMethods(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
                                .Any(methodInfo => methodInfo.IsDefined(typeof(ExtensionAttribute), true)))
                        {
                            foundExtensions.Add(type);
                        }
                    }
                }
                catch (Exception)
                {
                    // Ignore assemblies that can't be loaded
                }

                // ReSharper restore EmptyGeneralCatchClause
            }

            ExtensionTypes = foundExtensions.ToArray();
            return foundTypes.ToArray();
        }

        /// <summary>
        /// Get's the value of a internal or non-internal property or field of an 
        /// object, when supplied by name.
        /// </summary>
        /// <param name = "instance">
        /// The instance object from which the Field or Property should 
        /// be returned.
        /// </param>
        /// <param name = "name">
        /// The name of the Field or Property to retrieve.
        /// </param>
        /// <returns>
        /// The value of the Field or Property.
        /// </returns>
        internal static object Get(this object instance, string name)
        {
            if (instance == null)
            {
                throw new ArgumentNullException("instance");
            }
            return Get(instance.GetType(), instance, name);
        }

        /// <summary>
        /// Get's the value of a static internal or non-internal property or field of an 
        /// type, when supplied by name.
        /// </summary>
        /// <param name = "type">
        /// The type from which the Field or Property should be returned.
        /// </param>
        /// <param name = "name">
        /// The name of the Field or Property to retrieve.
        /// </param>
        /// <returns>
        /// The value of the Field or Property.
        /// </returns>
        internal static object Get(this Type type, string name)
        {
            return Get(type, null, name);
        }

        /// <summary>
        /// Get's the value of a internal or non-internal property or field of an 
        /// object, when supplied by name.
        /// </summary>
        /// <param name = "expando">
        /// The instance object from which the Field or Property should 
        /// be returned.
        /// </param>
        /// <param name = "name">
        /// The name of the Field or Property to retrieve.
        /// </param>
        /// <returns>
        /// The value of the Field or Property.
        /// </returns>
        internal static object Get(this IDictionary<string, object> expando, string name)
        {
            var asDictionary = expando.As<IDictionary<string, object>>();
            if (asDictionary.ContainsKey(name))
            {
                return asDictionary[name];
            }

            return null;
        }

        /// <summary>
        /// Invokes an Instance Method on a supplied object.
        /// </summary>
        /// <param name = "instance">
        /// The instance object on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this object instance, string methodName, params object[] parameters)
        {
            return Invoke(instance, methodName, typeof(string), parameters);
        }

        /// <summary>
        /// Invokes an Instance Method on a supplied object.
        /// </summary>
        /// <param name = "instance">
        /// The instance object on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "nullType">
        /// The type to use if a parameter is null.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this object instance, string methodName, Type nullType, params object[] parameters)
        {
            if (instance == null)
            {
                throw new ArgumentNullException("instance");
            }

            const BindingFlags Bindings = BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            Type type = instance.GetType();
            var bestMatch = LocateBestMatch<MethodInfo>(type, methodName, nullType, Bindings, parameters, null);

            if (bestMatch != null)
            {
                return InternalDynaInvoke(bestMatch, parameters, instance);
            }

            throw new MissingMethodException(string.Format(CultureInfo.InvariantCulture, "The method {0} does not exist", methodName));
        }

        /// <summary>
        /// Invokes a Generic Method with the supplied type Parameters and Argument Parameters.
        /// </summary>
        /// <param name = "instance">
        /// The instance object on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "typeParameters">
        /// The type parameters to use for the Generic Method.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this object instance, string methodName, Type[] typeParameters, params object[] parameters)
        {
            return Invoke(instance, methodName, typeof(string), typeParameters, parameters);
        }

        /// <summary>
        /// Invokes a Generic Method with the supplied type Parameters and Argument Parameters.
        /// </summary>
        /// <param name = "instance">
        /// The instance object on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "typeParameters">
        /// The type parameters to use for the Generic Method.
        /// </param>
        /// <param name = "nullType">
        /// The type to use if a parameter is null.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this object instance, string methodName, Type[] typeParameters, Type nullType, params object[] parameters)
        {
            if (instance == null)
            {
                throw new ArgumentNullException("instance");
            }
            const BindingFlags Bindings = BindingFlags.InvokeMethod | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            Type type = instance.GetType();

            var bestMatch = LocateBestMatch<MethodInfo>(type, methodName, nullType, Bindings, parameters, null);

            if (bestMatch != null)
            {
                MethodInfo generic = bestMatch.MakeGenericMethod(typeParameters);
                return InternalDynaInvoke(generic, parameters, instance);
            }

            throw new MissingMethodException(string.Format(CultureInfo.InvariantCulture, "The method {0} does not exist", methodName));
        }

        /// <summary>
        /// Invokes a static Method on a type.
        /// </summary>
        /// <param name = "type">
        /// The type on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this Type type, string methodName, params object[] parameters)
        {
            return Invoke(type, methodName, typeof(string), parameters);
        }

        /// <summary>
        /// Invokes a static Method on a type.
        /// </summary>
        /// <param name = "type">
        /// The Type on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "nullType">
        /// The type to use if a parameter is null.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this Type type, string methodName, Type nullType, params object[] parameters)
        {
            const BindingFlags Bindings = BindingFlags.InvokeMethod | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
            var bestMatch = LocateBestMatch<MethodInfo>(type, methodName, nullType, Bindings, parameters, null);

            if (bestMatch != null)
            {
                return InternalDynaInvoke(bestMatch, parameters, null);
            }

            throw new MissingMethodException(string.Format(CultureInfo.InvariantCulture, "The method {0} does not exist", methodName));
        }

        /// <summary>
        /// Invokes a static Generic Method with the supplied type Parameters and Argument Parameters.
        /// </summary>
        /// <param name = "type">
        /// The type on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "typeParameters">
        /// The type parameters to use for the Generic Method.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this Type type, string methodName, Type[] typeParameters, params object[] parameters)
        {
            return Invoke(type, methodName, typeof(string), typeParameters, parameters);
        }

        /// <summary>
        /// Invokes a static Generic Method with the supplied type Parameters and Argument Parameters.
        /// </summary>
        /// <param name = "type">
        /// The type on which the method should be invoked.
        /// </param>
        /// <param name = "methodName">
        /// The name of the Method to invoke.
        /// </param>
        /// <param name = "nullType">
        /// The type to use if a parameter is null.
        /// </param>
        /// <param name = "typeParameters">
        /// The type parameters to use for the Generic Method.
        /// </param>
        /// <param name = "parameters">
        /// The parameter objects to supply to the method.
        /// </param>
        /// <returns>
        /// The return value of the method invoked.
        /// </returns>
        internal static object Invoke(this Type type, string methodName, Type nullType, Type[] typeParameters, params object[] parameters)
        {
            const BindingFlags Bindings = BindingFlags.InvokeMethod | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

            var bestMatch = LocateBestMatch<MethodInfo>(type, methodName, nullType, Bindings, parameters, typeParameters);

            if (bestMatch != null)
            {
                MethodInfo generic = bestMatch.MakeGenericMethod(typeParameters);
                return InternalDynaInvoke(generic, parameters, null);
            }

            throw new MissingMethodException(string.Format(CultureInfo.InvariantCulture, "The method {0} does not exist", methodName));
        }

        /// <summary>
        /// Evaluates type compatibility.
        /// </summary>
        /// <typeparam name="T"> The type to evaluate against. </typeparam>
        /// <param name="inputValue"> The object to evaluate compatibility for. </param>
        /// <returns> True if the object is compatible otherwise false. </returns>
        internal static bool Is<T>(this object inputValue)
        {
            return inputValue is T;
        }

        /// <summary>
        /// Represents an extended set of "primitive" types that
        /// includes types that are not primitives but so basic to
        /// the CLR that they can generally be treated as primitives 
        /// (even though they are not necessarily all value types).
        /// The extended set of primitives includes the set of actual
        /// primitives.
        /// </summary>
        /// <param name="inputValue">
        /// The type to be evaluated.
        /// </param>
        /// <returns>
        /// True if the type is a primitive or "extended" primitive.
        /// </returns>
        internal static bool IsExtendedPrimitive(this Type inputValue)
        {
            if (inputValue == null)
            {
                throw new ArgumentNullException("inputValue");
            }

            return inputValue.IsPrimitive || ExtendedPrimitives.Contains(inputValue);
        }

        /// <summary>
        /// Determines if a type is a floating point type.
        /// </summary>
        /// <param name="inputValue">
        /// The type to evaluate.
        /// </param>
        /// <returns>
        /// True if the type is a numeric type otherwise false.
        /// </returns>
        internal static bool IsFloatingPoint(this Type inputValue)
        {
            return FloatTypes.Contains(inputValue);
        }

        /// <summary>
        /// Determines if a type is an integer type.
        /// </summary>
        /// <param name="inputValue">
        /// The type to evaluate.
        /// </param>
        /// <returns>
        /// True if the type is a numeric type otherwise false.
        /// </returns>
        internal static bool IsInteger(this Type inputValue)
        {
            return IntegerTypes.Contains(inputValue);
        }

        /// <summary>
        /// Determines whether the specified object is not null.
        /// </summary>
        /// <param name="inputValue"> The object. </param>
        /// <returns>
        /// <c>true</c> if the specified object is not null; otherwise, <c>false</c> .
        /// </returns>
        internal static bool IsNotNull([ValidatedNotNull] this object inputValue)
        {
            return !ReferenceEquals(inputValue, null);
        }

        /// <summary>
        /// Determines whether the specified object is null.
        /// </summary>
        /// <param name="inputValue"> The object. </param>
        /// <returns>
        /// <c>true</c> if the specified object is null; otherwise, <c>false</c> .
        /// </returns>
        internal static bool IsNull([ValidatedNotNull] this object inputValue)
        {
            return ReferenceEquals(inputValue, null);
        }

        /// <summary>
        /// Determines if a type is a numeric type.
        /// </summary>
        /// <param name="inputValue">
        /// The type to evaluate.
        /// </param>
        /// <returns>
        /// True if the type is a numeric type otherwise false.
        /// </returns>
        internal static bool IsNumeric(this Type inputValue)
        {
            return IsInteger(inputValue) || IsFloatingPoint(inputValue);
        }

        /// <summary>
        /// Determines whether [is params parameter] [the specified parameter info].
        /// </summary>
        /// <param name="parameterInfo">The parameter info.</param>
        /// <returns>
        ///   <c>true</c> if [is params parameter] [the specified parameter info]; otherwise, <c>false</c>.
        /// </returns>
        internal static bool IsParamsParameter(this ParameterInfo parameterInfo)
        {
            if (parameterInfo == null)
            {
                throw new ArgumentNullException("parameterInfo");
            }

            return parameterInfo.GetCustomAttributes(true).Where(a => a.GetType() == typeof(ParamArrayAttribute)).Count() > 0;
        }

        /// <summary>
        /// Determines if a type is a signed type.
        /// </summary>
        /// <param name="inputValue">
        /// The type to evaluate.
        /// </param>
        /// <returns>
        /// True if the type is signed otherwise false.
        /// </returns>
        internal static bool IsSigned(this Type inputValue)
        {
            return SignedTypes.Contains(inputValue);
        }

        /// <summary>
        /// Determines if the type is an unsigned type.
        /// </summary>
        /// <param name="inputValue">
        /// The type to evaluate.
        /// </param>
        /// <returns>
        /// True if the type is unsigned otherwise false.
        /// </returns>
        internal static bool IsUnsigned(this Type inputValue)
        {
            return UnsignedTypes.Contains(inputValue);
        }

        /// <summary>
        /// Sets any internal or non-internal property or field by name.
        /// </summary>
        /// <param name = "instance">
        /// The instance of the object.
        /// </param>
        /// <param name = "name">
        /// The name of the Field or Property to set.
        /// </param>
        /// <param name = "value">
        /// The value to be set.
        /// </param>
        internal static void Set(this object instance, string name, object value)
        {
            if (instance == null)
            {
                throw new ArgumentNullException("instance");
            }
            Set(instance.GetType(), instance, name, value);
        }

        /// <summary>
        /// Sets any internal or non-internal property or field by name.
        /// </summary>
        /// <param name = "type">
        /// The type for which the static property will be set.
        /// </param>
        /// <param name = "name">
        /// The name of the Field or Property to set.
        /// </param>
        /// <param name = "value">
        /// The value to be set.
        /// </param>
        internal static void Set(this Type type, string name, object value)
        {
            Set(type, null, name, value);
        }

        /// <summary>
        /// Sets any internal or non-internal property or field by name.
        /// </summary>
        /// <param name = "expando">
        /// The instance of the object.
        /// </param>
        /// <param name = "name">
        /// The name of the Field or Property to set.
        /// </param>
        /// <param name = "value">
        /// The value to be set.
        /// </param>
        internal static void Set(this IDictionary<string, object> expando, string name, object value)
        {
            expando.As<IDictionary<string, object>>()[name] = value;
        }

        /// <summary>
        /// Returns the substring that occurs after a given string.
        /// </summary>
        /// <param name="inputValue">
        /// The string to parse.
        /// </param>
        /// <param name="searchValue">
        /// The string to look for within the string to parse.
        /// </param>
        /// <returns>
        /// The portion of the string that appears after the string to parse or 
        /// the original string if the parse string was not found.
        /// </returns>
        internal static string SubstringAfter(this string inputValue, string searchValue)
        {
            if (inputValue == null)
            {
                throw new ArgumentNullException("inputValue");
            }
            int loc = inputValue.IndexOfOi(searchValue);
            if (loc >= 0 && loc < inputValue.Length + 1)
            {
                return inputValue.Substring(loc + 1);
            }

            return inputValue;
        }

        /// <summary>
        /// Returns the substring that occurs before a given string.
        /// </summary>
        /// <param name="inputValue">
        /// The string to parse.
        /// </param>
        /// <param name="searchValue">
        /// The string to look for within the string to parse.
        /// </param>
        /// <returns>
        /// The portion of the string that appears before the string to parse or 
        /// the original string if the parse string was not found.
        /// </returns>
        internal static string SubstringBefore(this string inputValue, string searchValue)
        {
            if (inputValue == null)
            {
                throw new ArgumentNullException("inputValue");
            }

            int loc = inputValue.IndexOfOi(searchValue);
            if (loc >= 0)
            {
                return inputValue.Substring(0, loc);
            }

            return inputValue;
        }


        /// <summary>
        /// Determines the compatibility of a match between a set of supplied 
        /// parameters and a set of possible parameters for an overloaded function.
        /// </summary>
        /// <param name = "parameters">
        /// A set of ParameterInfo objects representing the available parameters
        /// for an overload of a method.
        /// </param>
        /// <param name = "values">
        /// A set of objects representing a intended set of values to be supplied to
        /// an overload for an object.
        /// </param>
        /// <param name = "nullType">
        /// The type that should be used if one of the parameters are null.
        /// </param>
        /// <param name="typeParameters">
        /// The type parameters for a generic method.
        /// </param>
        /// <returns>
        /// A numerical indicator of the compatibility between a possible parameter
        /// set and a supplied set of objects.  The higher this indicator the stronger
        /// the match.
        /// </returns>
        [SuppressMessage("Microsoft.Maintainability", "CA1502:AvoidExcessiveComplexity",
            Justification = "NEIN: This should be refactored to reduce the complexity, letting it go for now. [tistocks]")]
        private static int CompareTypeAssignments(ParameterInfo[] parameters, object[] values, Type nullType, Type[] typeParameters)
        {
            TestHelp.DoNothing(typeParameters);

            int paramCount = parameters != null ? parameters.Count() : 0;
            int objCount = values != null ? values.Count() : 0;
            if (paramCount == 0 && objCount == 0)
            {
                return int.MaxValue;
            }

            int max = Math.Max(paramCount, objCount);
            int retval = 0;
            ParameterInfo paramsParameter = null;
            for (int i = 0; i < max; i++)
            {
                if (i < paramCount && i < objCount)
                {
                    ParameterInfo info = parameters[i];
                    if (info.GetCustomAttributes(true).Where(a => a.GetType().IsAssignableFrom(typeof(ParamArrayAttribute))).Count() > 0)
                    {
                        paramsParameter = info;
                    }

                    Type infoType = info.ParameterType;

                    object obj = values[i];
                    Type objType = nullType;

                    if (obj != null)
                    {
                        objType = obj.GetType();
                    }

                    // If the object type is exactly the same as the infoType we have an exact match.
                    if (objType == infoType)
                    {
                        retval += 6;
                        continue;
                    }

                    // Check to see if method parameter is of type T.
                    if (infoType.IsGenericParameter)
                    {
                        Type[] genericParameterConstraints = infoType.GetGenericParameterConstraints();

                        if (genericParameterConstraints.Count() == 0 ||
                            genericParameterConstraints.Any(constraint => constraint.IsAssignableFrom(objType)))
                        {
                            retval += 3;
                        }
                    }
                    else if (infoType.IsArray && objType.IsArray)
                    {
                        Type infoElementType = infoType.GetElementType();
                        Type objElementType = objType.GetElementType();
                        if (infoElementType.ContainsGenericParameters)
                        {
                            Type[] genericParameterConstraints = infoElementType.GetGenericParameterConstraints();

                            if (genericParameterConstraints.Count() == 0 ||
                                genericParameterConstraints.Any(constraint => constraint.IsAssignableFrom(objElementType)))
                            {
                                retval += 3;
                            }
                        }
                    }
                    else if (infoType.IsArray && paramsParameter.IsNotNull())
                    {
                        Type infoElementType = infoType.GetElementType();
                        Type objElementType = objType;
                        if (infoElementType.ContainsGenericParameters)
                        {
                            Type[] genericParameterConstraints = infoElementType.GetGenericParameterConstraints();

                            if (genericParameterConstraints.Count() == 0 ||
                                genericParameterConstraints.Any(constraint => constraint.IsAssignableFrom(objElementType)))
                            {
                                retval += 3;
                            }
                        }
                    }
                    else
                    {
                        if (infoType == objType)
                        {
                            retval += 6;
                        }
                        else if (infoType.IsAssignableFrom(objType))
                        {
                            retval += 5;
                        }
                        else if (objType.IsAssignableFrom(infoType))
                        {
                            retval += 5;
                        }
                        else if (infoType.ContainsGenericParameters)
                        {
                            // Above check to see if method parameter is of type C<T>/C<U,V>.

                            // Since infoType is obtained from compile time information
                            // it does not have T resolved. Hence we use infoType.Name 
                            // which is in format C'1.
                            // infoType.FullName is empty therefore it is not used.                            
                            if (infoType.Name == objType.Name)
                            {
                                retval += 3;
                            }
                        }
                        else if (typeof(IEnumerable).IsAssignableFrom(infoType) && typeof(IEnumerable).IsAssignableFrom(objType))
                        {
                            retval += 1;
                        }
                    }
                }
                else if (i >= paramCount && i < objCount && paramsParameter.IsNotNull())
                {
                    // params function, this method could be covered by the params array.
                    break;
                }
                else
                {
                    // parameter count is unequal so return the lowest score.
                    retval = 0;
                    break;
                }
            }

            return retval;
        }


        /// <summary>
        /// Get's a Field or Property value from an object or type given the name of the desired 
        /// Property or Field.
        /// </summary>
        /// <param name = "type">
        /// The type for which the static property will be returned.
        /// </param>
        /// <param name = "instance">
        /// The instance object from which the property should be requested.
        /// </param>
        /// <param name = "name">
        /// The name of the property or field.
        /// </param>
        /// <returns>
        /// The value of the property or field.
        /// </returns>
        private static object Get(Type type, object instance, string name)
        {
            if (type.Is<ExpandoObject>() || type.Is<IDictionary<string, object>>())
            {
                return Get(instance.As<IDictionary<string, object>>(), name);
            }

            BindingFlags staticOrInstance = instance.IsNull() ? BindingFlags.Static : BindingFlags.Instance;
            BindingFlags bindings = BindingFlags.Public | BindingFlags.NonPublic | staticOrInstance;
            MemberInfo info = type.GetMember(name, bindings | BindingFlags.GetField | BindingFlags.GetProperty).FirstOrDefault();

            if (info == null)
            {
                throw new MissingMemberException(string.Format(CultureInfo.InvariantCulture, "The member {0} does not exist", name));
            }

            Type infoType = info.GetType();

            if (infoType.Is<PropertyInfo>())
            {
                return Get(instance, (PropertyInfo)info);
            }

            if (infoType.Is<FieldInfo>())
            {
                return Get(instance, (FieldInfo)info);
            }

            throw new MissingMemberException("Member Not Found");
        }

        /// <summary>
        /// Get's a internal or non-internal property of an object given
        /// the PropertyInfo for the desired property.
        /// </summary>
        /// <param name = "instance">
        /// The instance of the object for which the property value is
        /// desired.
        /// </param>
        /// <param name = "property">
        /// The property desired.
        /// </param>
        /// <returns>
        /// The value of the property.
        /// </returns>
        private static object Get(object instance, PropertyInfo property)
        {
            object retval = property.GetValue(instance, null);
            return retval;
        }

        /// <summary>
        /// Get's a Field value for an object give the FieldInfo for the 
        /// desired field.
        /// </summary>
        /// <param name = "instance">
        /// The instance object for which the field value is desired.
        /// </param>
        /// <param name = "field">
        /// The FieldInfo object for the desired field.
        /// </param>
        /// <returns>
        /// The value of the Field desired.
        /// </returns>
        private static object Get(object instance, FieldInfo field)
        {
            return field.GetValue(instance);
        }

        /// <summary>
        /// Dynamically Invokes either a constructor, instance or static method as supplied with the
        /// provided arguments an instance as appropriate.
        /// </summary>
        /// <param name = "method">The method to invoke.</param>
        /// <param name = "parameters">The parameters to use.</param>
        /// <param name = "instance">The instance to invoke the method against if it is an instance method.</param>
        /// <returns>The results of the instance invocation.</returns>
        private static object InternalDynaInvoke(MethodBase method, object[] parameters, object instance = null)
        {
            try
            {
                if (typeof(ConstructorInfo).IsAssignableFrom(method.GetType()))
                {
                    return method.As<ConstructorInfo>().Invoke(parameters);
                }

                if (instance.IsNotNull() && method.IsStatic)
                {
                    return method.Invoke(null, TestHelp.Array(instance).Union(parameters).ToArray());
                }
                else
                {
                    return method.Invoke(instance, parameters);
                }
            }
            catch (TargetInvocationException ex)
            {
                // An exception occurring inside of the Target when Invoked via
                // reflection will get caught and rethrown as a TargetInvocationException
                // In these cases we want to have the inner exception sent up the
                // stack so that the actual exception is presented to the test case.
                // This code removes the TargetInvocationException and "fixes" the 
                // call stack to match.

                // Use reflections to get the inner field that holds the stack inside
                // of an exception.
                FieldInfo remoteStackTrace = typeof(Exception).GetField("_remoteStackTraceString", BindingFlags.Instance | BindingFlags.NonPublic);

                // Set the InnerException._remoteStackTraceString            
                // to the current InnerException.StackTrace
                // This "fixes" the stack trace so that it does not appear to originate 
                // from here when we re-throw the inner exception.
                remoteStackTrace.SetValue(ex.InnerException, ex.InnerException.StackTrace + Environment.NewLine);

                // Re-throw the inner exception.
                throw ex.InnerException;
            }
        }

        /// <summary>
        /// Locates the best match overload to utilize given a set of method call arguments.
        /// </summary>
        /// <typeparam name = "T">
        /// The type of objects to return, this should be either MethodInfo or ConstructorInfo.
        /// </typeparam>
        /// <param name = "type">
        /// The type for which the method should be located.
        /// </param>
        /// <param name = "name">
        /// The name of the method to locate (can be null for constructors).
        /// </param>
        /// <param name = "nullType">
        /// The type to "assume" for any inbound parameter that is null.
        /// </param>
        /// <param name = "bindings">
        /// The BindingFlags to use when locating methods.
        /// </param>
        /// <param name = "parameters">
        /// The parameters to use when locating methods overloads.
        /// </param>
        /// <param name="typeParameters">
        /// The type Parameters to be used with a generic method.
        /// </param>
        /// <returns>
        /// A MethodBase (of type T) representing the "best match" method to use for the supplied
        /// parameters.
        /// </returns>
        private static T LocateBestMatch<T>(Type type, string name, Type nullType, BindingFlags bindings, object[] parameters, Type[] typeParameters)
            where T : MethodBase
        {
            IEnumerable<T> infos = null;
            IEnumerable<T> extensions = null;
            if (typeof(MethodInfo).IsAssignableFrom(typeof(T)))
            {
                IEnumerable<MethodInfo> methods = type.GetMethods(bindings).Where(x => x.Name == name);
                IEnumerable<MethodInfo> extensionMethods =
                    ExtensionTypes.SelectMany(t => t.GetMethods()).Where(m => m.IsDefined(typeof(ExtensionAttribute), true));
                IEnumerable<MethodInfo> sameNamedExtensionMethods = extensionMethods.Where(m => m.Name == name);
                extensions = sameNamedExtensionMethods.Where(m => m.GetParameters()[0].ParameterType.IsAssignableFrom(type)).Cast<T>();

                infos = methods.Cast<T>();
            }
            else if (typeof(ConstructorInfo).IsAssignableFrom(typeof(T)))
            {
                ConstructorInfo[] constructors = type.GetConstructors(bindings);
                infos = constructors.Cast<T>();
            }

            T bestMatch = null;
            int bestScore = 0;

            // Attempt the find the best match for the method.
            foreach (T info in infos)
            {
                // This is used to "score" the best constructor to call.
                // This avoids unnecessary ambiguity errors when multiple members
                // "could work".
                int score = CompareTypeAssignments(info.GetParameters(), parameters, nullType, typeParameters);
                if (score > bestScore)
                {
                    bestScore = score;
                    bestMatch = info;
                }
            }

            // If no match, try to find a match from an extension method.
            if (bestMatch.IsNull() && extensions.IsNotNull() && typeof(MethodInfo).IsAssignableFrom(typeof(T)))
            {
                foreach (T info in extensions)
                {
                    // This is used to "score" the best constructor to call.
                    // This avoids unnecessary ambiguity errors when multiple members
                    // "could work".
                    int score = CompareTypeAssignments(info.GetParameters().Skip(1).ToArray(), parameters, nullType, null);
                    if (score > bestScore)
                    {
                        bestScore = score;
                        bestMatch = info;
                    }
                }
            }

            return bestMatch;
        }

        /// <summary>
        /// Sets an instance or a static property or field on a type or object.
        /// </summary>
        /// <param name = "type">
        /// The type for which the static property will be set.
        /// </param>
        /// <param name = "instance">
        /// An instance of the type on which to set an instance value.
        /// </param>
        /// <param name = "name">
        /// The name of the value to set.
        /// </param>
        /// <param name = "value">
        /// The value to assign.
        /// </param>
        private static void Set(Type type, object instance, string name, object value)
        {
            if (type.Is<IDictionary<string, object>>())
            {
                Set(instance.As<IDictionary<string, object>>(), name, value);
                return;
            }

            BindingFlags staticOrInstance = instance.IsNull() ? BindingFlags.Static : BindingFlags.Instance;
            BindingFlags bindings = BindingFlags.Public | BindingFlags.NonPublic | staticOrInstance;
            MemberInfo info = type.GetMember(name, bindings | BindingFlags.GetField | BindingFlags.GetProperty).FirstOrDefault();

            if (info == null)
            {
                throw new MissingMemberException(string.Format(CultureInfo.InvariantCulture, "The member {0} does not exist", name));
            }

            Type infoType = info.GetType();

            if (infoType.Is<PropertyInfo>())
            {
                Set(instance, (PropertyInfo)info, value);
            }
            else if (infoType.Is<FieldInfo>())
            {
                Set(instance, (FieldInfo)info, value);
            }
            else
            {
                throw new MissingMemberException("Member Not Found");
            }
        }

        /// <summary>
        /// Modifies a given property inside of an instantiated object.
        /// </summary>
        /// <param name = "instance">
        /// The object for which the property should be altered.
        /// </param>
        /// <param name = "property">
        /// The property to change.
        /// </param>
        /// <param name = "value">
        /// The value to set the property to.
        /// </param>
        private static void Set(object instance, PropertyInfo property, object value)
        {
            property.SetValue(instance, value, null);
        }

        /// <summary>
        /// Modifies a given property inside of an instantiated object.
        /// </summary>
        /// <param name = "instance">
        /// The object for which the property should be altered.
        /// </param>
        /// <param name = "field">
        /// The field to change.
        /// </param>
        /// <param name = "value">
        /// The value to set the property to.
        /// </param>
        private static void Set(object instance, FieldInfo field, object value)
        {
            field.SetValue(instance, value);
        }
    }
}
