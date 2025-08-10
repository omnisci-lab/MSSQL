using MSSQL.Connection;
using System;
using System.Collections.Generic;

namespace MSSQL.Access
{
    public partial class SqlContext : IDisposable
    {
        private bool disposedValue;
        private SqlExecHelper _sqlExecHelper;
        private Dictionary<string, object> _sqlAccessObjDict;

        public SqlContext()
        {
            _sqlExecHelper = new SqlExecHelper(SqlConnectInfo.GetConnectionString());
            _sqlExecHelper.Connect();

            _sqlAccessObjDict = new Dictionary<string, object>();
            disposedValue = false;
        }

        protected SqlAccess<T> InitSqlAccess<T>() where T : ISqlTable, new()
        {
            SqlAccess<T> sqlAccess = null;
            string key = typeof(SqlAccess<T>).ToString();
            if (_sqlAccessObjDict.TryGetValue(key, out object value))
            {
                sqlAccess = (value as SqlAccess<T>);
                sqlAccess.Reset();
                return sqlAccess;
            }

            sqlAccess = new SqlAccess<T>(_sqlExecHelper);
            _sqlAccessObjDict.Add(key, sqlAccess);

            return sqlAccess;
        }

        public SqlExecHelper GetHelper()
        {
            return _sqlExecHelper;
        }

        public void BeginTransaction()
        {
             _sqlExecHelper.BeginTransaction();
        }

        public void CommitTransaction()
        {
            _sqlExecHelper.CommitTransaction();
        }

        public void RollbackTransaction()
        {
            _sqlExecHelper.RollbackTransaction();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _sqlAccessObjDict.Clear();
                    _sqlAccessObjDict = null;
                }

                _sqlExecHelper.Dispose();
                disposedValue = true;
            }
        }

        ~SqlContext()
        {
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
