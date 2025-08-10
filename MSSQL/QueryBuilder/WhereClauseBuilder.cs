using MSSQL.Attributes;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace MSSQL.QueryBuilder
{
    public class WhereClauseBuilder<T> where T : ISqlTable, new()
    {
        private readonly List<SqlParameter> _parameters;
        private int _anonymousParamsCount;

        public WhereClauseBuilder(List<SqlParameter> parameters)
        {
            _parameters = parameters;
        }

        public string ParseExpression(Expression expression, PropertyInfo[] properties, List<string> paramKeys)
        {
            string paramKey = null;
            switch (expression)
            {
                case BinaryExpression binaryExpression:
                    return ParseBinaryExpression(binaryExpression, properties, paramKeys);
                case MemberExpression memberExpression:
                    if (memberExpression.Expression is ParameterExpression parameterExpr)
                        return ParseParameterExpr(memberExpression, properties);
                    //else if (memberExpression.Expression is ConstantExpression constantExpression)
                    //{
                    //    if (!(memberExpression.Member is FieldInfo fieldInfo))
                    //        throw new Exception($"");

                    //    object val = fieldInfo.GetValue(constantExpression.Value);
                    //    paramKey = $"@{memberExpression.Member.Name}";
                    //    _parameters.Add(new SqlParameter(paramKey, val ?? DBNull.Value));
                    //    paramKeys.Add(paramKey);

                    //    return paramKey;
                    //}
                    else
                    {
                        return ParseMemberExpr(memberExpression, properties, paramKeys);
                    }
                case ConstantExpression constantExpression:
                    paramKey = $"@val{++_anonymousParamsCount}";
                    _parameters.Add(new SqlParameter(paramKey, constantExpression.Value ?? DBNull.Value));
                    paramKeys.Add(paramKey);

                    return paramKey;
                case MethodCallExpression methodCallExpression:
                    if (methodCallExpression.Method.Name != "Contains" || !(methodCallExpression.Object is MemberExpression))
                        throw new Exception($"Method {methodCallExpression.Method.Name} is not supported in this context");

                    MemberExpression member = methodCallExpression.Object as MemberExpression;
                    PropertyInfo property2 = properties.FirstOrDefault(p => p.Name == member.Member.Name);
                    if (property2 is null)
                        throw new Exception($"Property {member.Member.Name} not found in type {typeof(T).Name}");

                    var valueExpr = methodCallExpression.Arguments[0];
                    object val2 = null;
                    if (valueExpr is ConstantExpression constExpr)
                        val2 = $"%{constExpr.Value}%";
                    else
                        val2 = $"%{Expression.Lambda(valueExpr).Compile().DynamicInvoke()}%";

                    paramKey = $"@contains{member.Member.Name}";
                    _parameters.Add(new SqlParameter(paramKey, val2 ?? DBNull.Value));

                    SqlColumnAttribute columnAttribute2 = property2.GetCustomAttribute<SqlColumnAttribute>();
                    return columnAttribute2 is null ?
                        $"[{member.Member.Name}] LIKE %@{member.Member.Name}%" : $"[{columnAttribute2.ColumnName}] LIKE @contains{member.Member.Name}";
                default:
                    throw new NotSupportedException($"Expression type {expression.GetType()} is not supported");
            }
        }

        private string ParseBinaryExpression(BinaryExpression binaryExpression, PropertyInfo[] properties, List<string> paramKeys)
        {
            var left = ParseExpression(binaryExpression.Left, properties, paramKeys);
            var right = ParseExpression(binaryExpression.Right, properties, paramKeys);
            var operatorString = GetSqlOperator(binaryExpression.NodeType);

            return $"{left} {operatorString} {right}";
        }

        private string GetSqlOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "!=";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.OrElse:
                    return "OR";
                default:
                    throw new NotSupportedException($"Operator {nodeType} not supported");
            }
        }


        private string ParseParameterExpr(MemberExpression memberExpr, PropertyInfo[] properties)
        {
            PropertyInfo property = properties.FirstOrDefault(p => p.Name == memberExpr.Member.Name);
            SqlColumnAttribute columnAttribute = property.GetCustomAttribute<SqlColumnAttribute>();

            return columnAttribute is null ? $"[{memberExpr.Member.Name}]" : $"[{columnAttribute.ColumnName}]";
        }

        /**
         * x => x.A == obj.A
         */
        private string ParseMemberExpr(MemberExpression expr, PropertyInfo[] properties, List<string> paramKeys)
        {
            if (expr.Member is PropertyInfo property)
            {
                string paramKey = $"@{property.Name}";

                object objVal = GetValueFromExpression(expr.Expression);
                object val = property.GetValue(objVal);
                _parameters.Add(new SqlParameter(paramKey, val ?? DBNull.Value));
                paramKeys.Add(paramKey);

                return paramKey;
            }

            if (expr.Member is FieldInfo field)
            {
                string paramKey = $"@{field.Name}";

                object objVal = GetValueFromExpression(expr.Expression);
                object val = field.GetValue(objVal);
                _parameters.Add(new SqlParameter(paramKey, val ?? DBNull.Value));
                paramKeys.Add(paramKey);

                return paramKey;
            }

            throw new NotSupportedException("");
        }

        private object GetValueFromExpression(Expression expr)
        {
            if (expr is ConstantExpression constExpr)
                return constExpr.Value;

            if (expr is MemberExpression memberExpr)
            {
                object target = GetValueFromExpression(memberExpr.Expression);

                if (memberExpr.Member is FieldInfo fi)
                    return fi.GetValue(target);

                if (memberExpr.Member is PropertyInfo pi)
                    return pi.GetValue(target);
            }

            throw new NotSupportedException("Cannot get value from expression type!");
        }
    }
}
