using MSSQL.Attributes;
using MSSQL.Cache;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
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
    public partial class SqlQueryBuilder<T>
    {
        /*** Select statement builder
         * 
         * @param selector
         * @return SqlQueryBuilder
         */
        public static SqlQueryBuilder<T> Select(Expression<Func<T, object>> selector = null)
        {
            SqlQueryBuilder<T> builder = new SqlQueryBuilder<T>(createInstance: true, getType: true);
            object selectorObject = null;
            string[] selectorColumnList = null;
            ClauseCacheKey cacheKey = new ClauseCacheKey { Type = builder._ttype, ClauseTypes = ClauseTypes.Select };

            if (selector != null)
            {
                selectorObject = selector.Compile().Invoke(builder._instance);
                selectorColumnList = selectorObject.GetType().GetProperties().Select(p => p.Name).ToArray();
                cacheKey.Expression = selector.ToString();
            }

            if(TryGetClause(cacheKey, out ClauseCacheValue cacheValue))
            {
                builder.query.Append(cacheValue.Clause);
                return builder;
            }

            ParseSelectClause(builder, selectorObject, selectorColumnList);
            AddClause(cacheKey, new ClauseCacheValue { Clause = builder.query.ToString() });

            return builder;
        }


        private static SqlQueryBuilder<T> ParseSelectClause(SqlQueryBuilder<T> builder, object selectorObject, string[] selectorColumnList)
        {
            builder.SetTableName();

            if (selectorObject is null)
            {
                builder.query.Append($"SELECT * FROM [{builder._tableName}]");
                return builder;
            }

            if (selectorColumnList.Length == 0)
                throw new ArgumentException("No columns selected");

            PropertyInfo[] tProperties = ReflectionCache.GetProperties<T>();
            List<string> columnList = new List<string>();
            foreach (string selectorColumn in selectorColumnList)
            {
                PropertyInfo tProperty = tProperties.FirstOrDefault(p => p.Name == selectorColumn);
                if (tProperty is null)
                    continue;

                SqlColumnAttribute sqlColumnAttribute = tProperty.GetCustomAttribute<SqlColumnAttribute>();
                if (sqlColumnAttribute is null)
                    columnList.Add($"[{selectorColumn}]");
                else
                    columnList.Add($"[{sqlColumnAttribute.ColumnName}]");
            }

            builder.query.Append($"SELECT {string.Join(", ", columnList)} FROM [{builder._tableName}]");

            return builder;
        }

        /*** Insert statement builder
         * 
         * @param record
         * @return SqlQueryBuilder
         */
        public static SqlQueryBuilder<T> Insert(T record)
        {
            SqlQueryBuilder<T> builder = new SqlQueryBuilder<T>(getType: true);
            ClauseCacheKey cacheKey = new ClauseCacheKey { Type = builder._ttype, ClauseTypes = ClauseTypes.Insert };
            bool cached = false;
            if(TryGetClause(cacheKey, out ClauseCacheValue cacheValue))
            {
                builder.query.Append(cacheValue.Clause);
                cached = true;
            }

            PropertyInfo[] recordProperties = ReflectionCache.GetProperties<T>();
            if (cached)
            {
                ParseInsertClause(builder, record, recordProperties);
                return builder;
            }

            ParseInsertClauseWithoutCache(builder, record, recordProperties);
            AddClause(cacheKey, new ClauseCacheValue { Clause = builder.query.ToString() });

            return builder;
        }

        private static SqlQueryBuilder<T> ParseInsertClause(SqlQueryBuilder<T> builder, T record, PropertyInfo[] recordProperties)
        {
            foreach (PropertyInfo property in recordProperties)
            {
                SqlColumnAttribute columnAttribute = property.GetCustomAttribute<SqlColumnAttribute>();
                string columnName = null;

                if (columnAttribute is null)
                {
                    columnName = property.Name;
                    builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                }
                else
                {
                    columnName = columnAttribute.ColumnName;
                    if (columnAttribute.PrimaryKey && columnAttribute.AutoIncrement)
                        continue;
                    else
                        builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                }
            }

            return builder;
        }

        private static SqlQueryBuilder<T> ParseInsertClauseWithoutCache(SqlQueryBuilder<T> builder, T record, PropertyInfo[] recordProperties)
        {
            builder.SetTableName();

            StringBuilder insertColumnBuilder = new StringBuilder();
            StringBuilder setValueBuilder = new StringBuilder();

            foreach (PropertyInfo property in recordProperties)
            {
                SqlColumnAttribute columnAttribute = property.GetCustomAttribute<SqlColumnAttribute>();
                string columnName = null;

                if (columnAttribute is null)
                {
                    columnName = property.Name;
                    insertColumnBuilder.Append($"[{columnName}], ");
                    setValueBuilder.Append($"@{columnName}, ");
                    builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                }
                else
                {
                    columnName = columnAttribute.ColumnName;
                    if (columnAttribute.PrimaryKey && columnAttribute.AutoIncrement)
                    {
                        continue;
                    }
                    else
                    {
                        insertColumnBuilder.Append($"[{columnName}], ");
                        setValueBuilder.Append($"@{columnName}, ");
                        builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                    }
                }
            }

            insertColumnBuilder.Length -= 2;
            setValueBuilder.Length -= 2;
            builder.query.Append($"INSERT INTO [{builder._tableName}] ({insertColumnBuilder}) VALUES ({setValueBuilder})");

            return builder;
        }

        /*** Update statement builder
         * 
         * @param record
         * @param selector
         * @return SqlQueryBuilder
         */
        public static SqlQueryBuilder<T> Update(T record, Expression<Func<T, object>> selector = null)
        {
            SqlQueryBuilder<T> builder = new SqlQueryBuilder<T>(getType: true);
            ClauseCacheKey cacheKey = new ClauseCacheKey { Type = builder._ttype, ClauseTypes = ClauseTypes.Update };
            object selectorObject = null;
            string[] selectorColumnList = null;

            if (selector != null)
            {
                selectorObject = selector.Compile().Invoke(record);
                selectorColumnList = selectorObject.GetType().GetProperties().Select(p => p.Name).ToArray();
                cacheKey.Expression = selector.ToString();
            }

            bool cached = false;
            if(TryGetClause(cacheKey, out ClauseCacheValue cacheValue))
            {
                builder.query.Append(cacheValue.Clause);
                cached = true;
            }

            PropertyInfo[] recordProperties = null;
            if (selectorObject is null)
                recordProperties = ReflectionCache.GetProperties<T>();
            else
            {
                if (selectorColumnList.Length == 0)
                    throw new ArgumentException("No columns selected");

                recordProperties = ReflectionCache.GetProperties<T>()
                    .Where(x => selectorColumnList.Any(columnName => columnName == x.Name)).ToArray();
            }

            if (cached) {
                ParseUpdateClause(builder, record, recordProperties);
                return builder;
            }

            ParseUpdateClauseWithoutCache(builder, record, recordProperties);
            AddClause(cacheKey, new ClauseCacheValue { Clause = builder.query.ToString() });

            return builder;
        }

        private static SqlQueryBuilder<T> ParseUpdateClause(SqlQueryBuilder<T> builder, T record, PropertyInfo[] recordProperties)
        {
            foreach (PropertyInfo property in recordProperties)
            {
                SqlColumnAttribute columnAttribute = property.GetCustomAttribute<SqlColumnAttribute>();
                string columnName = null;

                if (columnAttribute is null)
                {
                    columnName = property.Name;
                    builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                }
                else
                {
                    columnName = columnAttribute.ColumnName;
                    if (columnAttribute.PrimaryKey)
                        continue;
                    else
                        builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                }
            }

            return builder;
        }

        private static SqlQueryBuilder<T> ParseUpdateClauseWithoutCache(SqlQueryBuilder<T> builder, T record, PropertyInfo[] recordProperties)
        {
            builder.SetTableName();
            StringBuilder setValueBuilder = new StringBuilder();

            foreach (PropertyInfo property in recordProperties)
            {
                SqlColumnAttribute columnAttribute = property.GetCustomAttribute<SqlColumnAttribute>();
                string columnName = null;

                if (columnAttribute is null)
                {
                    columnName = property.Name;
                    setValueBuilder.Append($"[{columnName}] = @{columnName}, ");
                    builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                }
                else
                {
                    columnName = columnAttribute.ColumnName;
                    if (columnAttribute.PrimaryKey)
                    {
                        continue;
                    }
                    else
                    {
                        setValueBuilder.Append($"[{columnName}] = @{columnName}, ");
                        builder._parameters.Add(new SqlParameter($"@{columnName}", property.GetValue(record) ?? DBNull.Value));
                    }
                }
            }
            setValueBuilder.Length -= 2;
            builder.query.Append($"UPDATE [{builder._tableName}] SET {setValueBuilder}");

            return builder;
        }

        /***
         * 
         * @return SqlQueryBuilder
         */
        public static SqlQueryBuilder<T> Delete()
        {
            SqlQueryBuilder<T> builder = new SqlQueryBuilder<T>(getType: true);
            ClauseCacheKey cacheKey = new ClauseCacheKey { Type = builder._ttype, ClauseTypes = ClauseTypes.Delete };
            if(TryGetClause(cacheKey, out ClauseCacheValue cacheValue))
            {
                builder.query.Append(cacheValue.Clause);
                return builder;
            }

            ParseDeleteClause(builder);
            AddClause(cacheKey, new ClauseCacheValue { Clause = builder.query.ToString() });

            return builder;

        }


        private static SqlQueryBuilder<T> ParseDeleteClause(SqlQueryBuilder<T> builder)
        {
            builder.SetTableName();
            builder.query.Append($"DELETE FROM [{builder._tableName}]");

            return builder;
        }

        /*** 
         * 
         * @return SqlQueryBuilder
         */
        public static SqlQueryBuilder<T> Count()
        {
            SqlQueryBuilder<T> builder = new SqlQueryBuilder<T>(getType: true);
            ClauseCacheKey cacheKey = new ClauseCacheKey { Type = builder._ttype, ClauseTypes = ClauseTypes.SelectCount };
            if(TryGetClause(cacheKey, out ClauseCacheValue cacheValue))
            {
                builder.query.Append(cacheValue.Clause);

                return builder;
            }

            ParseCount(builder);
            AddClause(cacheKey, new ClauseCacheValue { Clause = builder.query.ToString() });

            return builder;
        }

        private static SqlQueryBuilder<T> ParseCount(SqlQueryBuilder<T> builder)
        {
            builder.SetTableName();
            builder.query.Append($"SELECT CAST(COUNT(*) AS BIGINT) FROM [{builder._tableName}]");

            return builder;
        }
    }
}
