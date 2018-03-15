namespace Sitecore.Support.ContentSearch.SolrProvider
{
    using System.Collections.Generic;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.ContentSearch.SolrProvider;
    using Sitecore.ContentSearch.SolrProvider.SolrOperations;
    using Sitecore.ContentSearch.Utilities;
    using StringExtensions;
    using System.Threading;
    using SolrNet;

    public class SwitchOnRebuildSolrCloudSearchIndex : Sitecore.ContentSearch.SolrProvider.SwitchOnRebuildSolrCloudSearchIndex
    {
        private readonly ISolrOperationsFactory solrOperationsFactory;

        private ISolrOperations<Dictionary<string, object>> rebuildSolrOperations;

        public SwitchOnRebuildSolrCloudSearchIndex(string name, string mainalias, string rebuildalias, string activecollection, string rebuildcollection, ISolrOperationsFactory solrOperationsFactory, IIndexPropertyStore propertyStore) : base(name, mainalias, rebuildalias, activecollection, rebuildcollection, solrOperationsFactory, propertyStore)
        {
            this.solrOperationsFactory = solrOperationsFactory;
        }

        protected override void PerformRebuild(bool resetIndex, bool optimizeOnComplete, IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            if (!this.ShouldStartIndexing(indexingOptions))
            {
                return;
            }

            using (new RebuildIndexingTimer(this.PropertyStore))
            {
                this.rebuildSolrOperations = this.solrOperationsFactory.GetSolrOperations(this.RebuildCore);

                // Clear collection before populating it.
                if (resetIndex)
                {
                    this.Reset();
                }

                using (IProviderUpdateContext context = this.CreateTemporaryCoreUpdateContext(this.rebuildSolrOperations))
                {
                    foreach (IProviderCrawler crawler in this.Crawlers)
                    {
                        crawler.RebuildFromRoot(context, IndexingOptions.Default, CancellationToken.None);
                    }

                    context.Commit();
                }

                if (optimizeOnComplete && SolrContentSearchManager.SolrSettings.OptimizeOnRebuildEnabled())
                {
                    CrawlingLog.Log.Debug("[Index={0}] Optimizing collection [Collection: {1}]".FormatWith(this.Name, this.RebuildCollection));
                    this.rebuildSolrOperations.Optimize();
                }
            }

            this.SwitchAliasesCollections();
            this.SetAliasesConfiguration();
            this.PreserveAliasesCollections();

            // Clear old collection.
            // NOTE: clearing old collection after it's rebuild might be overengineering.
            // However, if the indexes are big, it may save some disk space.
            // Though disk space must be large enough to hold double of the current index size as both collections need to hold data while index is being rebuilt.
            //if (resetIndex)
            //{
            //    this.Reset();
            //}
        }

        /// <summary>
        /// Sets main alias to the active collection and rebuild alias to the rebuild collection.
        /// </summary>
        private void SetAliasesConfiguration()
        {
            // Set primary alias (Core property value) to last preserved active collection.
            this.SetAlias(this.Core, this.ActiveCollection);

            // Set rebuild alias (RebuildCollection property value) to last preserved rebuild collection.
            this.SetAlias(this.RebuildCore, this.RebuildCollection);
        }

        /// <summary>
        /// Writes main alias and rebuild alias values to the index property store.
        /// </summary>
        private void PreserveAliasesCollections()
        {
            this.PropertyStore.Set(SolrContentSearchManager.RebuildCollection, this.RebuildCollection);
            this.PropertyStore.Set(SolrContentSearchManager.ActiveCollection, this.ActiveCollection);
        }

        /// <summary>
        /// Swaps aliases collections of main and rebuild aliases.
        /// </summary>
        private void SwitchAliasesCollections()
        {
            var activeCore = this.ActiveCollection.Clone().ToString();
            this.ActiveCollection = this.RebuildCollection;
            this.RebuildCollection = activeCore;
            CrawlingLog.Log.Debug("[Index={0}] SwitchAliasesCollections: AliaseActiveCore switched to [Index={1}]  and RebuildCollection switched to [Index={2}].".FormatWith(this.Name, this.ActiveCollection, this.RebuildCollection));
        }
    }
}