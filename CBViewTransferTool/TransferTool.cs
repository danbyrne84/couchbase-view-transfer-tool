using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Couchbase.Configuration;
using Couchbase.Management;
using Newtonsoft.Json.Linq;

namespace CBViewTransferTool
{
    public class TransferTool
    {
        public void Backup(string srcNode, string bucket, string backupLocation)
        {
            var docsAndViews = RetrieveDesignDocs(srcNode, bucket);
            SaveDesignDocs(docsAndViews, backupLocation);
        }

        public void Transfer(string srcNode, string srcBucket, string targetNode, string targetBucket, string targetUser, string targetPass)
        {
            var targetConfig = new CouchbaseClientConfiguration {Username = targetUser, Password = targetPass};
            targetConfig.Urls.Add(new Uri(targetNode));

            var targetCluster = new CouchbaseCluster(targetConfig);

            var docsAndViews = RetrieveDesignDocs(srcNode, srcBucket);
            TransferViews(targetCluster, targetBucket, targetNode, docsAndViews);
        }

        public void Restore(string backupPath, string targetNode, string targetBucket, string targetUsername, string targetPassword)
        {
            var targetConfig = new CouchbaseClientConfiguration {Username = targetUsername, Password = targetPassword};
            targetConfig.Urls.Add(new Uri(targetNode));

            var targetCluster = new CouchbaseCluster(targetConfig);

            var docsAndViews = RetrieveLocalDesignDocs(backupPath);
            TransferViews(targetCluster, targetBucket, targetNode, docsAndViews);
        }

        protected void TransferViews(CouchbaseCluster targetCluster, string bucket, string nodeName, Dictionary<string, JToken> docsAndViews)
        {
            Console.WriteLine(String.Format("Transferring design documents to cluster at {0}", nodeName));

            // blat them accross to the target node
            foreach (var ddoc in docsAndViews)
            {
                Console.WriteLine("Deleting design document {0}", ddoc.Key);
                targetCluster.DeleteDesignDocument(bucket, ddoc.Key.Replace("_design", ""));

                Console.WriteLine("Creating design document {0}", ddoc.Key);
                targetCluster.CreateDesignDocument(bucket, ddoc.Key.Replace("_design", ""), ddoc.Value.ToString(Newtonsoft.Json.Formatting.None));
            }
        }

        protected Dictionary<string, JToken> RetrieveDesignDocs(string node, string bucket)
        {
            Console.WriteLine("Retrieving design documents from bucket {0} in the cluster at {1}", bucket, node);
            var json = new WebClient().DownloadString(node + String.Format("default/buckets/{0}/ddocs", bucket));
            var j = JObject.Parse(json);

            var ddocList = new Dictionary<string, JToken>();

            foreach (var row in j["rows"])
            {
                var ddocName = row["doc"]["meta"]["id"].ToString();
                var viewList = row["doc"]["json"];

                ddocList.Add(ddocName, viewList);
            }

            return ddocList;
        }

        protected Dictionary<string, JToken> RetrieveLocalDesignDocs(string backupLocation)
        {
            Console.WriteLine("Restoring views from local backup");

            var docsAndViews = new Dictionary<string, JToken>();
            foreach (var f in new DirectoryInfo(backupLocation).GetFiles())
            {
                using (var inFile = new StreamReader(backupLocation + "\\" + f.Name))
                {
                    Console.WriteLine("Restoring backup of design document {0}", f.Name);
                    var json = inFile.ReadToEnd();
                    docsAndViews[f.Name] = JToken.Parse(json);
                }
            }

            return docsAndViews;
        }

        protected void SaveDesignDocs(Dictionary<string, JToken> docsAndViews, string backupLocation)
        {
            Console.WriteLine("Backing up design documents...");

            if (!Directory.Exists(backupLocation))
            {
                Console.WriteLine("Backup location {0} doesn't exist, creating", backupLocation);
                Directory.CreateDirectory(backupLocation);
            }

            foreach (var ddoc in docsAndViews)
            {
                var ddocName = ddoc.Key.Replace("_design/","");
                using (var outfile = new StreamWriter(backupLocation + "\\" + ddocName))
                {
                    Console.WriteLine("Creating backup of design document {0}", ddocName);
                    outfile.Write(ddoc.Value.ToString(Newtonsoft.Json.Formatting.None));
                }
            }
        }
    }
}