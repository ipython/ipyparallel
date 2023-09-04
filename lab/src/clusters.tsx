import {
  showErrorMessage,
  Toolbar,
  ToolbarButton,
  CommandToolbarButton,
} from "@jupyterlab/apputils";

import { IChangedArgs } from "@jupyterlab/coreutils";

import * as nbformat from "@jupyterlab/nbformat";

import { ServerConnection } from "@jupyterlab/services";

import { refreshIcon } from "@jupyterlab/ui-components";

import { ArrayExt } from "@lumino/algorithm";

import { JSONObject, JSONExt, MimeData } from "@lumino/coreutils";

import { ElementExt } from "@lumino/domutils";

import { Drag } from "@lumino/dragdrop";

import { Message } from "@lumino/messaging";

import { Poll } from "@lumino/polling";

import { ISignal, Signal } from "@lumino/signaling";

import { Widget, PanelLayout } from "@lumino/widgets";

import { newClusterDialog, INewCluster } from "./dialog";

import * as React from "react";
import { createRoot } from "react-dom/client";
import { CommandRegistry } from "@lumino/commands";

import { CommandIDs } from "./commands";

/**
 * A refresh interval (in ms) for polling the backend cluster manager.
 */
const REFRESH_INTERVAL = 5000;

/**
 * The threshold in pixels to start a drag event.
 */
const DRAG_THRESHOLD = 5;

/**
 * The mimetype used for Jupyter cell data.
 */
const JUPYTER_CELL_MIME = "application/vnd.jupyter.cells";

const CLUSTER_PREFIX = "ipyparallel/clusters";
/**
 * A widget for IPython cluster management.
 */
export class ClusterManager extends Widget {
  /**
   * Create a new cluster manager.
   */

  constructor(options: ClusterManager.IOptions) {
    super();
    this.addClass("ipp-ClusterManager");

    this._serverSettings = ServerConnection.makeSettings();
    this._injectClientCodeForCluster = options.injectClientCodeForCluster;
    this._getClientCodeForCluster = options.getClientCodeForCluster;
    this._registry = options.registry;

    // A function to set the active cluster.
    this._setActiveById = (id: string) => {
      const cluster = this._clusters.find((c) => c.id === id);
      if (!cluster) {
        return;
      }

      const old = this._activeCluster;
      if (old && old.id === cluster.id) {
        return;
      }
      this._activeCluster = cluster;
      this._activeClusterChanged.emit({
        name: "cluster",
        oldValue: old,
        newValue: cluster,
      });
      this.update();
    };

    const layout = (this.layout = new PanelLayout());

    this._clusterListing = new Widget();
    this._clusterListing.addClass("ipp-ClusterListing");

    // Create the toolbar.
    const toolbar = new Toolbar<Widget>();

    // Make a label widget for the toolbar.
    const toolbarLabel = new Widget();
    toolbarLabel.node.textContent = "CLUSTERS";
    toolbarLabel.addClass("ipp-ClusterManager-label");
    toolbar.addItem("label", toolbarLabel);

    // Make a refresh button for the toolbar.
    toolbar.addItem(
      "refresh",
      new ToolbarButton({
        icon: refreshIcon,
        onClick: async () => {
          return this._updateClusterList();
        },
        tooltip: "Refresh Cluster List",
      }),
    );

    // Make a new cluster button for the toolbar.
    toolbar.addItem(
      CommandIDs.newCluster,
      new CommandToolbarButton({
        commands: this._registry,
        id: CommandIDs.newCluster,
      }),
    );

    layout.addWidget(toolbar);
    layout.addWidget(this._clusterListing);

    // Do an initial refresh of the cluster list.
    void this._updateClusterList();
    // Also refresh periodically.
    this._poll = new Poll({
      factory: async () => {
        await this._updateClusterList();
      },
      frequency: { interval: REFRESH_INTERVAL, backoff: true, max: 60 * 1000 },
      standby: "when-hidden",
    });
  }

  /**
   * The currently selected cluster, or undefined if there is none.
   */
  get activeCluster(): IClusterModel | undefined {
    return this._activeCluster;
  }

  /**
   * Set an active cluster by id.
   */
  setActiveCluster(id: string): void {
    this._setActiveById(id);
  }

  /**
   * A signal that is emitted when an active cluster changes.
   */
  get activeClusterChanged(): ISignal<
    this,
    IChangedArgs<IClusterModel | undefined>
  > {
    return this._activeClusterChanged;
  }

  /**
   * Whether the cluster manager is ready to launch a cluster
   */
  get isReady(): boolean {
    return this._isReady;
  }

  /**
   * Get the current clusters known to the manager.
   */
  get clusters(): IClusterModel[] {
    return this._clusters;
  }

  /**
   * Refresh the current list of clusters.
   */
  async refresh(): Promise<void> {
    await this._updateClusterList();
  }

  /**
   * Create a new cluster.
   */
  async create(): Promise<IClusterModel> {
    const clusterRequest = await newClusterDialog({});
    if (!clusterRequest) {
      return;
    }
    const cluster = await this._newCluster(clusterRequest);
    return cluster;
  }

  /**
   * Start a cluster by ID.
   */
  async start(id: string): Promise<void> {
    const cluster = this._clusters.find((c) => c.id === id);
    if (!cluster) {
      throw Error(`Cannot find cluster ${id}`);
    }
    await this._startById(id);
  }

  /**
   * Stop a cluster by ID.
   */
  async stop(id: string): Promise<void> {
    const cluster = this._clusters.find((c) => c.id === id);
    if (!cluster) {
      throw Error(`Cannot find cluster ${id}`);
    }
    await this._stopById(id);
  }

  /**
   * Scale a cluster by ID.
   */
  async scale(id: string): Promise<IClusterModel> {
    const cluster = this._clusters.find((c) => c.id === id);
    if (!cluster) {
      throw Error(`Cannot find cluster ${id}`);
    }
    const newCluster = await this._scaleById(id);
    return newCluster;
  }

  /**
   * Dispose of the cluster manager.
   */
  dispose(): void {
    if (this.isDisposed) {
      return;
    }
    this._poll.dispose();
    super.dispose();
  }

  /**
   * Handle an update request.
   */
  protected onUpdateRequest(msg: Message): void {
    // Don't bother if the sidebar is not visible
    if (!this.isVisible) {
      return;
    }
    const root = createRoot(this._clusterListing.node);
    root.render(
      <ClusterListing
        clusters={this._clusters}
        activeClusterId={(this._activeCluster && this._activeCluster.id) || ""}
        scaleById={(id: string) => {
          return this._scaleById(id);
        }}
        startById={(id: string) => {
          return this._startById(id);
        }}
        stopById={(id: string) => {
          return this._stopById(id);
        }}
        setActiveById={this._setActiveById}
        injectClientCodeForCluster={this._injectClientCodeForCluster}
      />,
    );
  }

  /**
   * Rerender after showing.
   */
  protected onAfterShow(msg: Message): void {
    this.update();
  }

  /**
   * Handle `after-attach` messages for the widget.
   */
  protected onAfterAttach(msg: Message): void {
    super.onAfterAttach(msg);
    let node = this._clusterListing.node;
    node.addEventListener("p-dragenter", this);
    node.addEventListener("p-dragleave", this);
    node.addEventListener("p-dragover", this);
    node.addEventListener("mousedown", this);
  }

  /**
   * Handle `before-detach` messages for the widget.
   */
  protected onBeforeDetach(msg: Message): void {
    let node = this._clusterListing.node;
    node.removeEventListener("p-dragenter", this);
    node.removeEventListener("p-dragleave", this);
    node.removeEventListener("p-dragover", this);
    node.removeEventListener("mousedown", this);
    document.removeEventListener("mouseup", this, true);
    document.removeEventListener("mousemove", this, true);
  }

  /**
   * Handle the DOM events for the directory listing.
   *
   * @param event - The DOM event sent to the widget.
   *
   * #### Notes
   * This method implements the DOM `EventListener` interface and is
   * called in response to events on the panel's DOM node. It should
   * not be called directly by user code.
   */
  handleEvent(event: Event): void {
    switch (event.type) {
      case "mousedown":
        this._evtMouseDown(event as MouseEvent);
        break;
      case "mouseup":
        this._evtMouseUp(event as MouseEvent);
        break;
      case "mousemove":
        this._evtMouseMove(event as MouseEvent);
        break;
      default:
        break;
    }
  }

  /**
   * Handle `mousedown` events for the widget.
   */
  private _evtMouseDown(event: MouseEvent): void {
    const { button, shiftKey } = event;

    // We only handle main or secondary button actions.
    if (!(button === 0 || button === 2)) {
      return;
    }
    // Shift right-click gives the browser default behavior.
    if (shiftKey && button === 2) {
      return;
    }

    // Find the target cluster.
    const clusterIndex = this._findCluster(event);
    if (clusterIndex === -1) {
      return;
    }
    // Prepare for a drag start
    this._dragData = {
      pressX: event.clientX,
      pressY: event.clientY,
      index: clusterIndex,
    };

    // Enter possible drag mode
    document.addEventListener("mouseup", this, true);
    document.addEventListener("mousemove", this, true);
    event.preventDefault();
  }

  /**
   * Handle the `'mouseup'` event on the document.
   */
  private _evtMouseUp(event: MouseEvent): void {
    // Remove the event listeners we put on the document
    if (event.button !== 0 || !this._drag) {
      document.removeEventListener("mousemove", this, true);
      document.removeEventListener("mouseup", this, true);
    }
    event.preventDefault();
    event.stopPropagation();
  }

  /**
   * Handle the `'mousemove'` event for the widget.
   */
  private _evtMouseMove(event: MouseEvent): void {
    let data = this._dragData;
    if (!data) {
      return;
    }
    // Check for a drag initialization.
    let dx = Math.abs(event.clientX - data.pressX);
    let dy = Math.abs(event.clientY - data.pressY);
    if (dx >= DRAG_THRESHOLD || dy >= DRAG_THRESHOLD) {
      event.preventDefault();
      event.stopPropagation();
      void this._startDrag(data.index, event.clientX, event.clientY);
    }
  }

  /**
   * Start a drag event.
   */
  private async _startDrag(
    index: number,
    clientX: number,
    clientY: number,
  ): Promise<void> {
    // Create the drag image.
    const model = this._clusters[index];
    const listingItem = this._clusterListing.node.querySelector(
      `li.ipp-ClusterListingItem[data-cluster-id="${model.id}"]`,
    ) as HTMLElement;
    const dragImage = Private.createDragImage(listingItem);

    // Set up the drag event.
    this._drag = new Drag({
      mimeData: new MimeData(),
      dragImage,
      supportedActions: "copy",
      proposedAction: "copy",
      source: this,
    });

    // Add mimeData for plain text so that normal editors can
    // receive the data.
    const textData = this._getClientCodeForCluster(model);
    this._drag.mimeData.setData("text/plain", textData);
    // Add cell data for notebook drops.
    const cellData: nbformat.ICodeCell[] = [
      {
        cell_type: "code",
        source: textData,
        outputs: [],
        execution_count: null,
        metadata: {},
      },
    ];
    this._drag.mimeData.setData(JUPYTER_CELL_MIME, cellData);

    // Remove mousemove and mouseup listeners and start the drag.
    document.removeEventListener("mousemove", this, true);
    document.removeEventListener("mouseup", this, true);
    return this._drag.start(clientX, clientY).then((action) => {
      if (this.isDisposed) {
        return;
      }
      this._drag = null;
      this._dragData = null;
    });
  }

  /**
   * Launch a new cluster on the server.
   */
  private async _newCluster(
    clusterRequest: INewCluster,
  ): Promise<IClusterModel> {
    this._isReady = false;
    this._registry.notifyCommandChanged(CommandIDs.newCluster);
    // TODO: allow requesting a profile, options
    const response = await ServerConnection.makeRequest(
      `${this._serverSettings.baseUrl}${CLUSTER_PREFIX}`,
      { method: "POST", body: JSON.stringify(clusterRequest) },
      this._serverSettings,
    );
    if (response.status !== 200) {
      const err = await response.json();
      void showErrorMessage("Cluster Create Error", err);
      this._isReady = true;
      this._registry.notifyCommandChanged(CommandIDs.newCluster);
      throw err;
    }
    const model = (await response.json()) as IClusterModel;
    await this._updateClusterList();
    this._isReady = true;
    this._registry.notifyCommandChanged(CommandIDs.newCluster);
    return model;
  }

  /**
   * Refresh the list of clusters on the server.
   */
  private async _updateClusterList(): Promise<void> {
    const response = await ServerConnection.makeRequest(
      `${this._serverSettings.baseUrl}${CLUSTER_PREFIX}`,
      {},
      this._serverSettings,
    );
    if (response.status !== 200) {
      const msg =
        "Failed to list clusters: might the server extension not be installed/enabled?";
      const err = new Error(msg);
      if (!this._serverErrorShown) {
        void showErrorMessage("IPP Extension Server Error", err);
        this._serverErrorShown = true;
      }
      throw err;
    }
    const data = (await response.json()) as IClusterModel[];
    this._clusters = data;

    // Check to see if the active cluster still exits.
    // If it doesn't, or if there is no active cluster,
    // select the first one.
    const active = this._clusters.find(
      (c) => c.id === (this._activeCluster && this._activeCluster.id),
    );
    if (!active) {
      const id = (this._clusters[0] && this._clusters[0].id) || "";
      this._setActiveById(id);
    }
    this.update();
  }

  /**
   * Start a cluster by its id.
   */
  private async _startById(id: string): Promise<void> {
    const response = await ServerConnection.makeRequest(
      `${this._serverSettings.baseUrl}${CLUSTER_PREFIX}/${id}`,
      { method: "POST" },
      this._serverSettings,
    );
    if (response.status > 299) {
      const err = await response.json();
      void showErrorMessage("Failed to start cluster", err);
      throw err;
    }
    await this._updateClusterList();
  }

  /**
   * Stop a cluster by its id.
   */
  private async _stopById(id: string): Promise<void> {
    const response = await ServerConnection.makeRequest(
      `${this._serverSettings.baseUrl}${CLUSTER_PREFIX}/${id}`,
      { method: "DELETE" },
      this._serverSettings,
    );
    if (response.status !== 204) {
      const err = await response.json();
      void showErrorMessage("Failed to close cluster", err);
      throw err;
    }
    await this._updateClusterList();
  }

  /**
   * Scale a cluster by its id.
   */
  private async _scaleById(id: string): Promise<IClusterModel> {
    const cluster = this._clusters.find((c) => c.id === id);
    if (!cluster) {
      throw Error(`Failed to find cluster ${id} to scale`);
    }
    // TODO: scale not implemented
    // should add an engine set
    void showErrorMessage("Scale not implemented", "");

    // const update = await showScalingDialog(cluster);

    const update = cluster;

    if (JSONExt.deepEqual(update, cluster)) {
      // If the user canceled, or the model is identical don't try to update.
      return Promise.resolve(cluster);
    }

    const response = await ServerConnection.makeRequest(
      `${this._serverSettings.baseUrl}${CLUSTER_PREFIX}/${id}`,
      {
        method: "PATCH",
        body: JSON.stringify(update),
      },
      this._serverSettings,
    );
    if (response.status !== 200) {
      const err = await response.json();
      void showErrorMessage("Failed to scale cluster", err);
      throw err;
    }
    const model = (await response.json()) as IClusterModel;
    await this._updateClusterList();
    return model;
  }

  private _findCluster(event: MouseEvent): number {
    const nodes = Array.from(
      this.node.querySelectorAll("li.ipp-ClusterListingItem"),
    );
    return ArrayExt.findFirstIndex(nodes, (node) => {
      return ElementExt.hitTest(node, event.clientX, event.clientY);
    });
  }

  private _drag: Drag | null;
  private _dragData: {
    pressX: number;
    pressY: number;
    index: number;
  } | null = null;
  private _clusterListing: Widget;
  private _clusters: IClusterModel[] = [];
  private _activeCluster: IClusterModel | undefined;
  private _setActiveById: (id: string) => void;
  private _injectClientCodeForCluster: (model: IClusterModel) => void;
  private _getClientCodeForCluster: (model: IClusterModel) => string;
  private _poll: Poll;
  private _serverSettings: ServerConnection.ISettings;
  private _activeClusterChanged = new Signal<
    this,
    IChangedArgs<IClusterModel | undefined>
  >(this);
  private _serverErrorShown = false;
  private _isReady = true;
  private _registry: CommandRegistry;
}

/**
 * A namespace for DasClusterManager statics.
 */
export namespace ClusterManager {
  /**
   * Options for the constructor.
   */
  export interface IOptions {
    /**
     * Registry of all commands
     */
    registry: CommandRegistry;

    /**
     * A callback to inject client connection cdoe.
     */
    injectClientCodeForCluster: (model: IClusterModel) => void;

    /**
     * A callback to get client code for a cluster.
     */
    getClientCodeForCluster: (model: IClusterModel) => string;
  }
}

/**
 * A React component for a launcher button listing.
 */
function ClusterListing(props: IClusterListingProps) {
  let listing = props.clusters.map((cluster) => {
    return (
      <ClusterListingItem
        isActive={cluster.id === props.activeClusterId}
        key={cluster.id}
        cluster={cluster}
        scale={() => props.scaleById(cluster.id)}
        start={() => props.startById(cluster.id)}
        stop={() => props.stopById(cluster.id)}
        setActive={() => props.setActiveById(cluster.id)}
        injectClientCode={() => props.injectClientCodeForCluster(cluster)}
      />
    );
  });

  // Return the JSX component.
  return (
    <div>
      <ul className="ipp-ClusterListing-list">{listing}</ul>
    </div>
  );
}

/**
 * Props for the cluster listing component.
 */
export interface IClusterListingProps {
  /**
   * A list of items to render.
   */
  clusters: IClusterModel[];

  /**
   * The id of the active cluster.
   */
  activeClusterId: string;

  /**
   * A function for starting a cluster by ID.
   */
  startById: (id: string) => Promise<void>;

  /**
   * A function for stopping a cluster by ID.
   */
  stopById: (id: string) => Promise<void>;

  /**
   * Scale a cluster by id.
   */
  scaleById: (id: string) => Promise<IClusterModel>;

  /**
   * A callback to set the active cluster by id.
   */
  setActiveById: (id: string) => void;

  /**
   * A callback to inject client code for a cluster.
   */
  injectClientCodeForCluster: (model: IClusterModel) => void;
}

/**
 * A TSX functional component for rendering a single running cluster.
 */
function ClusterListingItem(props: IClusterListingItemProps) {
  const { cluster, isActive, setActive, scale, start, stop, injectClientCode } =
    props;
  let itemClass = "ipp-ClusterListingItem";
  itemClass = isActive ? `${itemClass} jp-mod-active` : itemClass;

  let cluster_state = "Stopped";
  if (cluster.controller) {
    cluster_state = cluster.controller.state.state;
    if (cluster_state == "after") {
      cluster_state = "Stopped";
    }
  }

  // stop action is 'delete' for already-stopped clusters
  let STOP = cluster_state === "Stopped" ? "DELETE" : "STOP";

  return (
    <li
      className={itemClass}
      data-cluster-id={cluster.id}
      onClick={(evt) => {
        setActive();
        evt.stopPropagation();
      }}
    >
      <div className="ipp-ClusterListingItem-title">{cluster.id}</div>
      <div className="ipp-ClusterListingItem-stats">State: {cluster_state}</div>
      <div className="ipp-ClusterListingItem-stats">
        Number of engines: {cluster.engines.n || cluster.cluster.n || "auto"}
      </div>
      <div className="ipp-ClusterListingItem-button-panel">
        <button
          className="ipp-ClusterListingItem-button ipp-ClusterListingItem-code ipp-CodeIcon jp-mod-styled"
          onClick={(evt) => {
            injectClientCode();
            evt.stopPropagation();
          }}
          title={`Inject client code for ${cluster.id}`}
        />
        <button
          className={`ipp-ClusterListingItem-button ipp-ClusterListingItem-start jp-mod-styled ${
            cluster_state == "Stopped" ? "" : "ipp-hidden"
          }`}
          onClick={async (evt) => {
            evt.stopPropagation();
            return start();
          }}
          title={`Start ${cluster.id}`}
        >
          START
        </button>
        <button
          className="ipp-ClusterListingItem-button ipp-ClusterListingItem-scale jp-mod-styled ipp-hidden"
          onClick={async (evt) => {
            evt.stopPropagation();
            return scale();
          }}
          title={`Rescale ${cluster.id}`}
        >
          SCALE
        </button>
        <button
          className={`ipp-ClusterListingItem-button ipp-ClusterListingItem-stop jp-mod-styled ${
            cluster_state === "Stopped" && cluster.cluster.cluster_id === ""
              ? "ipp-hidden"
              : ""
          }`}
          onClick={async (evt) => {
            evt.stopPropagation();
            return stop();
          }}
          title={STOP}
        >
          {STOP}
        </button>
      </div>
    </li>
  );
}

/**
 * Props for the cluster listing component.
 */
export interface IClusterListingItemProps {
  /**
   * A cluster model to render.
   */
  cluster: IClusterModel;

  /**
   * Whether the cluster is currently active.
   */
  isActive: boolean;

  /**
   * A function for starting the cluster.
   */
  start: () => Promise<void>;

  /**
   * A function for scaling the cluster.
   */
  scale: () => Promise<IClusterModel>;

  /**
   * A function for stopping the cluster.
   */
  stop: () => Promise<void>;

  /**
   * A callback function to set the active cluster.
   */
  setActive: () => void;

  /**
   * A callback to inject client code into an editor.
   */
  injectClientCode: () => void;
}

/**
 * An interface for a JSON-serializable representation of a cluster.
 */
export interface IClusterModel extends JSONObject {
  /**
   * A unique string ID for the cluster.
   */
  id: string;

  /**
   * the cluster file for `Cluster.from_file`
   */
  cluster_file: string;

  cluster: {
    /**
     * The number of engines (null = auto)
     */
    n?: number;
    /**
     * The cluster id
     */
    cluster_id: string;

    /**
     * The profile directory
     */
    profile_dir: string;

    /**
     * The profile, abbreviated
     */
    profle: string;

    /**
     * The class import string
     */
    class: string;
  };

  controller?: {
    class: string;
    state?: {
      state: string;
    };
  };

  engines: {
    n: number;
    class: string;
    sets: {
      [key: string]: {
        n: number;
        state: string;
      };
    };
  };
}

/**
 * A namespace for module-private functionality.
 */
namespace Private {
  /**
   * Create a drag image for an HTML node.
   */
  export function createDragImage(node: HTMLElement): HTMLElement {
    const image = node.cloneNode(true) as HTMLElement;
    image.classList.add("ipp-ClusterListingItem-drag");
    return image;
  }
}
