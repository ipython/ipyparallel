import { Widget, PanelLayout } from "@lumino/widgets";
import { CommandRegistry } from "@lumino/commands";

import { ClusterManager, IClusterModel } from "./clusters";

/**
 * A widget for hosting IPP cluster widgets
 */
export class Sidebar extends Widget {
  /**
   * Create a new IPP sidebar.
   */
  constructor(options: Sidebar.IOptions) {
    super();
    this.addClass("ipp-Sidebar");
    let layout = (this.layout = new PanelLayout());

    const injectClientCodeForCluster = options.clientCodeInjector;
    const getClientCodeForCluster = options.clientCodeGetter;
    // Add the cluster manager component.
    this._clusters = new ClusterManager({
      registry: options.registry,
      injectClientCodeForCluster,
      getClientCodeForCluster,
    });
    layout.addWidget(this._clusters);
  }

  /**
   * Get the cluster manager associated with the sidebar.
   */
  get clusterManager(): ClusterManager {
    return this._clusters;
  }

  private _clusters: ClusterManager;
}

/**
 * A namespace for Sidebar statics.
 */
export namespace Sidebar {
  /**
   * Options for the constructor.
   */
  export interface IOptions {
    /**
     * Registry of all commands
     */
    registry: CommandRegistry;

    /**
     * A function that injects client-connection code for a given cluster.
     */
    clientCodeInjector: (model: IClusterModel) => void;

    /**
     * A function that gets client-connection code for a given cluster.
     */
    clientCodeGetter: (model: IClusterModel) => string;
  }
}
