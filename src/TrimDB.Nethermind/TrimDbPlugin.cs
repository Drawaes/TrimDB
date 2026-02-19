using System.Threading.Tasks;
using Autofac;
using Autofac.Core;
using Nethermind.Api;
using Nethermind.Api.Extensions;
using Nethermind.Core;
using Nethermind.Db;

namespace TrimDB.Nethermind;

public class TrimDbPlugin : INethermindPlugin
{
    public string Name => "TrimDB";
    public string Description => "Replaces RocksDB with TrimDB as the storage backend";
    public string Author => "TrimDB";
    public bool Enabled => true;
    public IModule? Module => new TrimDbModule();

    private class TrimDbModule : Autofac.Module
    {
        protected override void Load(ContainerBuilder builder)
        {
            // Override the default RocksDbFactory with TrimDbFactory.
            // Autofac last-registration-wins: plugin modules load after DbModule,
            // so this replaces the RocksDbFactory singleton.
            builder
                .Register(ctx =>
                {
                    IInitConfig initConfig = ctx.Resolve<IInitConfig>();
                    return new TrimDbFactory(initConfig.BaseDbPath);
                })
                .As<IDbFactory>()
                .SingleInstance();
        }
    }
}
