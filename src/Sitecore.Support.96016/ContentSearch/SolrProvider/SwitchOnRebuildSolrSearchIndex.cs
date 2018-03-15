using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Sitecore.Support.ContentSearch.SolrProvider
{
    using System.Threading;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.ContentSearch.Utilities;
    using SolrNet;

    public class SwitchOnRebuildSolrSearchIndex : Sitecore.ContentSearch.SolrProvider.SwitchOnRebuildSolrSearchIndex
    {
        public SwitchOnRebuildSolrSearchIndex(string name, string core, string rebuildcore, IIndexPropertyStore propertyStore) : base(name, core, rebuildcore, propertyStore)
        {
        }

        protected override void PerformRebuild(bool resetIndex, bool optimizeOnComplete, IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            if (!this.ShouldStartIndexing(indexingOptions))
                return;


            using (new RebuildIndexingTimer(this.PropertyStore))
            {
                if (resetIndex)
                {
                    this.Reset(this.tempSolrOperations, this.RebuildCore);
                }

                using (var context = this.CreateTemporaryCoreUpdateContext(this.tempSolrOperations))
                {
                    foreach (var crawler in this.Crawlers)
                    {
                        crawler.RebuildFromRoot(context, indexingOptions, cancellationToken);
                    }

                    context.Commit();
                }

                if (optimizeOnComplete)
                {
                    CrawlingLog.Log.Debug(string.Format("[Index={0}] Optimizing core [Core: {1}]", this.Name, this.RebuildCore));
                    this.tempSolrOperations.Optimize();
                }
            }

            this.SwapCores();
        }
        private void Reset(ISolrOperations<Dictionary<string, object>> operations, string coreName)
        {
            CrawlingLog.Log.Debug(string.Format("[Index={0}] Resetting index records [Core: {1}]", this.Name, coreName));

            var query = new SolrQueryByField("_indexname", this.Name);
            operations.Delete(query);
            operations.Commit();
        }

        private void SwapCores()
        {
            CrawlingLog.Log.Debug(string.Format("[Index={0}] Swapping cores [{1} -> {2}]", this.Name, this.Core, this.RebuildCore));

            var response = this.solrAdmin.Swap(this.Core, this.RebuildCore);

            if (response.Status != 0)
            {
                CrawlingLog.Log.Error(string.Format("[Index={0}] Error swapping cores. [{1}]", this.Name, this.RebuildCore));
            }
        }
    }
}