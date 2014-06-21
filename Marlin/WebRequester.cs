namespace Marlin
{
    using System;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;

    public class WebRequester
    {
        private const string RestEndpointBase = "hbaserest/";
        private readonly ClusterCredentials _credentials;
        private readonly CredentialCache _credentialCache;
        private readonly string _contentType;

        public WebRequester(ClusterCredentials credentials, string contentType = "application/x-protobuf")
        {
            _credentials = credentials;
            _contentType = contentType;
            _credentialCache = new CredentialCache();
            InitCache();
        }

        public HttpWebResponse IssueWebRequest(string endpoint, string method = "GET", Stream input = null)
        {
            return IssueWebRequestAsync(endpoint, method, input).Result;
        }

        public async Task<HttpWebResponse> IssueWebRequestAsync(string endpoint, string method = "GET", Stream input = null)
        {
            var httpWebRequest = WebRequest.CreateHttp(new Uri(_credentials.ClusterUri, RestEndpointBase + endpoint));
            httpWebRequest.Credentials = _credentialCache;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Method = method;
            httpWebRequest.Accept = _contentType;
            httpWebRequest.ContentType = _contentType;

            if (input != null)
            {
                using (var req = httpWebRequest.GetRequestStream())
                {
                    await input.CopyToAsync(req);
                }
            }

            return (await httpWebRequest.GetResponseAsync()) as HttpWebResponse;
        }

        private void InitCache()
        {
            _credentialCache.Add(_credentials.ClusterUri, "Basic",
                new NetworkCredential(_credentials.UserName, _credentials.ClusterPassword));
        }
    }
}
