namespace Marlin
{
    using System;
    using System.IO;
    using System.Net;
    using System.Threading.Tasks;

    public class WebRequester
    {
        private readonly ClusterCredentials _credentials;
        private readonly string _contentType;

        public WebRequester(ClusterCredentials credentials, string contentType = "application/x-protobuf")
        {
            _credentials = credentials;
            _contentType = contentType;
        }

        public Stream IssueWebRequest(string endpoint, Stream input = null)
        {
            return IssueWebRequestAsync(endpoint, input).Result;
        }

        public async Task<Stream> IssueWebRequestAsync(string endpoint, Stream input = null)
        {
            var httpWebRequest = WebRequest.CreateHttp(new Uri(_credentials.ClusterUri, endpoint));
            httpWebRequest.Credentials = new NetworkCredential(_credentials.UserName, _credentials.ClusterPassword);
            httpWebRequest.ContentType = _contentType;

            if (input != null)
            {
                using (var req = httpWebRequest.GetRequestStream())
                {
                    await input.CopyToAsync(req);
                }
            }

            using (var response = await httpWebRequest.GetResponseAsync())
            {
                return response.GetResponseStream();
            }
        }
    }
}
