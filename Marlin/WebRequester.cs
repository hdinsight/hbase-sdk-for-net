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

        public Stream IssueWebRequest(string endpoint, Stream input = null)
        {
            return IssueWebRequestAsync(endpoint, input).Result;
        }

        public async Task<Stream> IssueWebRequestAsync(string endpoint, Stream input = null)
        {
            var httpWebRequest = WebRequest.CreateHttp(new Uri(_credentials.ClusterUri, RestEndpointBase + endpoint));
            httpWebRequest.Credentials = _credentialCache;
            httpWebRequest.PreAuthenticate = true;
            httpWebRequest.Accept = _contentType;

            if (input != null)
            {
                using (var req = httpWebRequest.GetRequestStream())
                {
                    await input.CopyToAsync(req);
                }
            }

            return (await httpWebRequest.GetResponseAsync()).GetResponseStream();
        }

        private void InitCache()
        {
            _credentialCache.Add(_credentials.ClusterUri, "Basic",
                new NetworkCredential(_credentials.UserName, _credentials.ClusterPassword));
        }
    }
}
