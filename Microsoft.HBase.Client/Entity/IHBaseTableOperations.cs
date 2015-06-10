using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client.Entity
{
    interface IHBaseTableOperations<T>
    {
        Task<bool> CreateTableAsync(bool failIfExist, bool updateSchemaIfExist);
        Task DeleteTableAsync();

        Task InsertOrUpdateAsync(T entity);
        Task DeleteAsync(T entity);
        Task InsertOrUpdateRangeAsync(IEnumerable<T> entities);
        Task DeleteRangeAsync(IEnumerable<T> entities);
    }
}
