using MSSQL.Attributes;
using MSSQL.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using static MSSQL.QueryBuilder.SqlQueryCache;

namespace MSSQL.QueryBuilder
{
    /*** SqlQueryBuilder class
    * 
    * This class is used to build SQL queries
    * Author: Phan Xuân Chánh { Chinese Charater: 潘春正, EnglishName1: Chanh Xuan Phan, EnglishName2: StevePhan }
    *  - www.phanxuanchanh.com
    *  
    */
    public partial class SqlQueryBuilder<T> : SqlQueryBuilderBase where T : ISqlTable, new()
    {
        private readonly bool _createInstance;
        private readonly bool _getType;

        private readonly Type _ttype;
        private readonly T _instance;

        private WhereClauseBuilder<T> _whereClauseBuilder;

        protected SqlQueryBuilder(bool createInstance = false, bool getType = false)
            : base()
        {
            _createInstance = createInstance;
            _getType = getType;

            if (createInstance)
                _instance = ReflectionCache.GetObject<T>();

            if (getType)
                _ttype = typeof(T);

            _whereClauseBuilder = new WhereClauseBuilder<T>(_parameters);
        }

        /*** Set table name
         * 
         * @param ttype
         */
        private void SetTableName()
        {
            SqlTableAttribute tableAttribute = _ttype.GetCustomAttribute<SqlTableAttribute>();
            _tableName = tableAttribute is null ? _ttype.Name : tableAttribute.TableName;
        }

        public SqlQueryBuilder<T> Where(Expression<Func<T, bool>> expression)
        {
            if (expression == null)
                throw new ArgumentNullException(nameof(expression));

            PropertyInfo[] tProperties = ReflectionCache.GetProperties<T>();
            List<string> paramKeys = new List<string>();
            string where = _whereClauseBuilder.ParseExpression(expression.Body, tProperties, paramKeys);

            foreach (string paramKey in paramKeys)
            {
                string pattern = $@"\[(\w+)\]\s*=\s*{Regex.Escape(paramKey)}";

                Match match = Regex.Match(where, pattern);
                if (match.Success)
                {
                    string matchedWhere = match.Captures[0].Value;
                    object paramVal = _parameters.Where(x => x.ParameterName == paramKey).Select(s => s.Value).Single();
                    if (paramVal is DBNull)
                    {
                        string columnName = matchedWhere.Replace($"= {paramKey}", "");
                        where = where.Replace(matchedWhere, $"{columnName} IS NULL");
                    }
                }
            }

            conditions.Add(where);

            return this;
        }

        /*** 
         * 
         * @return SqlQueryBuilder
         */
        public SqlQueryBuilder<T> OrderBy(Expression<Func<T, object>> orderBy, bool descending = true)
        {
            if (orderBy == null)
                throw new ArgumentNullException(nameof(orderBy));

            if (_instance == null)
                throw new NullReferenceException(nameof(_instance));

            if (_ttype == null)
                throw new NullReferenceException(nameof(_ttype));

            ClauseCacheKey cacheKey = new ClauseCacheKey { Type = _ttype, Expression = orderBy.ToString(), ClauseTypes = ClauseTypes.OrderBy };
            if(TryGetClause(cacheKey, out ClauseCacheValue cacheValue))
            {
                _orderClause.Add(cacheValue.Clause);
                return this;
            }

            string orderClause = ParseOrderByClause(orderBy, descending);
            _orderClause.Add(orderClause);
            AddClause(cacheKey, new ClauseCacheValue { Clause = orderClause });

            return this;
        }

        private string ParseOrderByClause(Expression<Func<T, object>> orderBy, bool descending = false)
        {
            MemberExpression memberExpr = null;

            if (orderBy.Body is MemberExpression)
                memberExpr = orderBy.Body as MemberExpression;
            else
            {
                if (orderBy.Body is UnaryExpression unaryExpr)
                    memberExpr = unaryExpr.Operand as MemberExpression;
            }

            if (memberExpr is null || !(memberExpr.Member is PropertyInfo))
                throw new NotSupportedException("");

            PropertyInfo property = memberExpr.Member as PropertyInfo;
            SqlColumnAttribute sqlColumnAttribute = property.GetCustomAttribute<SqlColumnAttribute>();

            string orderClause = null;
            if (sqlColumnAttribute is null)
                orderClause = (descending) ? $"[{property.Name}] DESC" : $"[{property.Name}] ASC";
            else
                orderClause = (descending) ? $"[{sqlColumnAttribute.ColumnName}] DESC" : $"[{sqlColumnAttribute.ColumnName}] ASC";

            return orderClause;
        }
    }
}
