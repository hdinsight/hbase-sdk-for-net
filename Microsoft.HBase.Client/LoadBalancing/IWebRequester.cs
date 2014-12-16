using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace Microsoft.HBase.Client
{
    public interface IWebRequester
    {
        HttpWebResponse IssueWebRequest(string endpoint, string method, Stream input);

        Task<HttpWebResponse> IssueWebRequestAsync(string endpoint, string method, Stream input, string alternativeEndpointBase);
    }
}