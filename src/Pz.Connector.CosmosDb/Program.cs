using Pz.Connector.CosmosDb;
using Pz.Connectors.Sdk;

return await PzConnectorHost.RunAsync(args, ctx => new CosmosConnector(ctx.LoggerFactory)).ConfigureAwait(false);
