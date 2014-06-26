namespace Marlin
{
    using System;

    public class ScannerInformation
    {
        public string TableName { get; internal set; }

        public Uri Location { get; internal set; }

        public string ScannerId
        {
            get
            {
                return Location.PathAndQuery.Substring(Location.PathAndQuery.LastIndexOf('/'));
            }
        }

    }
}
