using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Web;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Maintenance;

namespace Sitecore.Support.ContentSearch.LuceneProvider
{
    public class SwitchOnRebuildLuceneIndex : Sitecore.ContentSearch.LuceneProvider.SwitchOnRebuildLuceneIndex
    {
        public SwitchOnRebuildLuceneIndex(string name, string folder, IIndexPropertyStore propertyStore) : base(name, folder, propertyStore)
        {
        }

        protected SwitchOnRebuildLuceneIndex(string name) : base(name)
        {
        }

        protected override void DoRebuild(IProviderUpdateContext context, IndexingOptions indexingOptions, CancellationToken cancellationToken)
        {
            this.VerifyNotDisposed();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            lock (this.GetFullRebuildLockObject())
            {
                foreach (IProviderCrawler crawler in (IEnumerable<IProviderCrawler>)this.Crawlers)
                    crawler.RebuildFromRoot(context, indexingOptions, cancellationToken);
                if ((this.IndexingState & IndexingState.Stopped) != IndexingState.Stopped)
                    context.Optimize();
                context.Commit();
                context.Optimize();
                stopwatch.Stop();
                if ((this.IndexingState & IndexingState.Stopped) == IndexingState.Stopped)
                    return;
                this.PropertyStore.Set(IndexProperties.RebuildTime, stopwatch.ElapsedMilliseconds.ToString((IFormatProvider)CultureInfo.InvariantCulture));
            }

            if ((this.IndexingState & IndexingState.Stopped) == IndexingState.Stopped)
            {
                CrawlingLog.Log.Debug(string.Format("[Index={0}] Swapping of cores was not done since indexing was stopped...", this.Name));
                return;
            }

            this.SwitchDirectories();
        }

    }
}