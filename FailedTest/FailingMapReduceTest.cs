using System;
using System.Collections.Generic;
using System.Linq;
using FailedTest.RavenTests;
using NUnit.Framework;
using FluentAssertions;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;

namespace FailedTest
{
    public class FailingMapReduceTest : RavenTestBase
    {
        [Test]
        public void CalledIndex_WhenIndexIsUsd_ShouldReturnAListOfClicks()
        {
            var snapshots = new[]
                {
                    new DroneStateSnapshoot
                        {
                            ClickActions = new List<ClickAction>
                                {
                                    new ClickAction {ContactId = "contact/1", CreativeId = "creative/1"},
                                    new ClickAction {ContactId = "contact/2", CreativeId = "creative/1"}
                                }
                        },
                    new DroneStateSnapshoot
                        {
                            ClickActions = new List<ClickAction>
                                {
                                    new ClickAction {ContactId = "contact/100", CreativeId = "creative/1"},
                                    new ClickAction {ContactId = "contact/200", CreativeId = "creative/1"}
                                }
                        },
                    new DroneStateSnapshoot
                        {
                            ClickActions = new List<ClickAction>
                                {
                                    new ClickAction {ContactId = "contact/1000", CreativeId = "creative/2"},
                                    new ClickAction {ContactId = "contact/2000", CreativeId = "creative/2"}
                                }
                        },
                    new DroneStateSnapshoot
                        {
                            ClickActions = new List<ClickAction>
                                {
                                    new ClickAction {ContactId = "contact/4000", CreativeId = "creative/2"},
                                    new ClickAction {ContactId = "contact/5000", CreativeId = "creative/2"}
                                }
                        }
                }.ToList();

            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    snapshots.ForEach(session.Store);
                    session.SaveChanges();
                }

                new Creatives_ClickActions().Execute(store);

                using (var session = store.OpenSession())
                {
                    var result =
                        session
                            .Query<Creatives_ClickActions.ReduceResult, Creatives_ClickActions>()
                            .Customize(customization => customization.WaitForNonStaleResults())
                            .ToList();

                    result.Should().Contain(x => x.CreativeId == "creative/1");
                    result.Should().Contain(x => x.CreativeId == "creative/2");
                    result.Should().OnlyContain(x => x.ClickedBy.Count() == 2);
                }
            }
        }

        public class DroneStateSnapshoot
        {
            public IList<ClickAction> ClickActions { get; set; }
        }

        public class ClickAction
        {
            public string ContactId { get; set; }
            public string CreativeId { get; set; }
            public DateTime Date { get; set; }
        }

        public class Creatives_ClickActions : AbstractIndexCreationTask<DroneStateSnapshoot, Creatives_ClickActions.ReduceResult>
        {
            public class ReduceResult
            {
                public string CreativeId { get; set; }
                public string[] ClickedBy { get; set; }
            }

            public Creatives_ClickActions()
            {
                Map = snapshots => snapshots
                                       .SelectMany(x => x.ClickActions)
                                       .Select(x => new { ClickedBy = x.ContactId, x.CreativeId });

                Reduce = result => result
                                       .GroupBy(x => x.CreativeId)
                                       .Select(
                                           x =>
                                           new
                                               {
                                                   ClickedBy = x.SelectMany(m => m.ClickedBy).ToArray(),
                                                   CreativeId = x.Key
                                               });
            }
        }
    }
}
