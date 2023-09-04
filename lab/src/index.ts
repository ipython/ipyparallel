// IPython Parallel Lab extension derived from dask-labextension@f6141455d770ed7de564fc4aa403b9964cd4e617
// License: BSD-3-Clause

import { PageConfig } from "@jupyterlab/coreutils";

import { TabPanel } from "@lumino/widgets";

import {
  ILabShell,
  JupyterFrontEnd,
  JupyterFrontEndPlugin,
} from "@jupyterlab/application";

import { ISessionContext, IWidgetTracker } from "@jupyterlab/apputils";

import { CodeEditor } from "@jupyterlab/codeeditor";

import { ConsolePanel, IConsoleTracker } from "@jupyterlab/console";

import { ISettingRegistry } from "@jupyterlab/settingregistry";

import { IStateDB } from "@jupyterlab/statedb";

import {
  INotebookTracker,
  NotebookActions,
  NotebookPanel,
} from "@jupyterlab/notebook";

import { Kernel, KernelMessage, Session } from "@jupyterlab/services";

import { LabIcon } from "@jupyterlab/ui-components";

import { Signal } from "@lumino/signaling";

import { IClusterModel, ClusterManager } from "./clusters";

import { Sidebar } from "./sidebar";

import "../style/index.css";

import logoSvgStr from "../style/logo.svg";

import { CommandIDs } from "./commands";

const PLUGIN_ID = "ipyparallel-labextension:plugin";
const CATEGORY = "IPython Parallel";

/**
 * The IPython Parallel extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  activate,
  id: PLUGIN_ID,
  requires: [IConsoleTracker, INotebookTracker, ISettingRegistry, IStateDB],
  optional: [ILabShell],
  autoStart: true,
};

/**
 * Export the plugin as default.
 */
export default plugin;

/**
 * Activate the cluster launcher plugin.
 */
async function activate(
  app: JupyterFrontEnd,
  consoleTracker: IConsoleTracker,
  notebookTracker: INotebookTracker,
  settingRegistry: ISettingRegistry,
  state: IStateDB,
  labShell?: ILabShell | null,
): Promise<void> {
  const id = "ipp-cluster-launcher";

  const isLab = !!labShell;
  const isRetroTree = PageConfig.getOption("retroPage") == "tree";

  const clientCodeInjector = async (model: IClusterModel) => {
    const editor = await Private.getCurrentEditor(
      app,
      notebookTracker,
      consoleTracker,
    );
    if (!editor) {
      return;
    }
    Private.injectClientCode(model, editor);
  };

  // Create the sidebar panel.
  const sidebar = new Sidebar({
    clientCodeInjector,
    clientCodeGetter: Private.getClientCode,
    registry: app.commands,
  });
  sidebar.id = id;
  sidebar.title.icon = new LabIcon({
    name: "ipyparallel:logo",
    svgstr: logoSvgStr,
  });

  // sidebar.title.iconClass = 'ipp-Logo jp-SideBar-tabIcon';

  if (isLab) {
    labShell.add(sidebar, "left", { rank: 200 });
    sidebar.title.caption = CATEGORY;
  } else if (isRetroTree) {
    const tabPanel = app.shell.currentWidget as TabPanel;
    tabPanel.addWidget(sidebar);
    tabPanel.tabBar.addTab(sidebar.title);
    sidebar.title.label = CATEGORY;
  }

  sidebar.clusterManager.activeClusterChanged.connect(async () => {
    const active = sidebar.clusterManager.activeCluster;
    return state.save(id, {
      cluster: active ? active.id : "",
    });
  });

  // A function to create a new client for a session.
  const createClientForSession = async (
    session: Session.ISessionConnection | null,
  ) => {
    if (!session) {
      return;
    }
    const cluster = sidebar.clusterManager.activeCluster;
    if (!cluster || !(await Private.shouldUseKernel(session.kernel))) {
      return;
    }
    return Private.createClientForKernel(cluster, session.kernel!);
  };

  type SessionOwner = NotebookPanel | ConsolePanel;
  // An array of the trackers to check for active sessions.
  const trackers: IWidgetTracker<SessionOwner>[] = [
    notebookTracker,
    consoleTracker,
  ];

  // A function to recreate a client on reconnect.
  const injectOnSessionStatusChanged = async (
    sessionContext: ISessionContext,
  ) => {
    if (
      sessionContext.session &&
      sessionContext.session.kernel &&
      sessionContext.session.kernel.status === "restarting"
    ) {
      return createClientForSession(sessionContext.session);
    }
  };

  // A function to inject a client when a new session owner is added.
  const injectOnWidgetAdded = (
    _: IWidgetTracker<SessionOwner>,
    widget: SessionOwner,
  ) => {
    widget.sessionContext.statusChanged.connect(injectOnSessionStatusChanged);
  };

  // A function to inject a client when the active cluster changes.
  const injectOnClusterChanged = () => {
    trackers.forEach((tracker) => {
      tracker.forEach(async (widget) => {
        const session = widget.sessionContext.session;
        if (session && (await Private.shouldUseKernel(session.kernel))) {
          return createClientForSession(session);
        }
      });
    });
  };

  // Whether the cluster clients should aggressively inject themselves
  // into the current session.
  let autoStartClient: boolean = false;

  // Update the existing trackers and signals in light of a change to the
  // settings system. In particular, this reacts to a change in the setting
  // for auto-starting cluster client.
  const updateTrackers = () => {
    // Clear any existing signals related to the auto-starting.
    Signal.clearData(injectOnWidgetAdded);
    Signal.clearData(injectOnSessionStatusChanged);
    Signal.clearData(injectOnClusterChanged);

    if (autoStartClient) {
      // When a new console or notebook is created, inject
      // a new client into it.
      trackers.forEach((tracker) => {
        tracker.widgetAdded.connect(injectOnWidgetAdded);
      });

      // When the status of an existing notebook changes, reinject the client.
      trackers.forEach((tracker) => {
        tracker.forEach(async (widget) => {
          await createClientForSession(widget.sessionContext.session);
          widget.sessionContext.statusChanged.connect(
            injectOnSessionStatusChanged,
          );
        });
      });

      // When the active cluster changes, reinject the client.
      sidebar.clusterManager.activeClusterChanged.connect(
        injectOnClusterChanged,
      );
    }
  };

  // Fetch the initial state of the settings.
  void Promise.all([settingRegistry.load(PLUGIN_ID), state.fetch(id)]).then(
    async (res) => {
      const settings = res[0];
      if (!settings) {
        console.warn("Unable to retrieve ipp-labextension settings");
        return;
      }
      const state = res[1] as { cluster?: string } | undefined;
      const cluster = state ? state.cluster : "";

      const onSettingsChanged = () => {
        // Determine whether to use the auto-starting client.
        // autoStartClient = settings.get("autoStartClient").composite as boolean;
        updateTrackers();
      };
      onSettingsChanged();
      // React to a change in the settings.
      settings.changed.connect(onSettingsChanged);

      // If an active cluster is in the state, reset it.
      if (cluster) {
        await sidebar.clusterManager.refresh();
        sidebar.clusterManager.setActiveCluster(cluster);
      }
    },
  );

  // Add a command to inject client connection code for a given cluster model.
  // This looks for a cluster model in the application context menu,
  // and looks for an editor among the currently active notebooks and consoles.
  // If either is not found, it bails.
  app.commands.addCommand(CommandIDs.injectClientCode, {
    label: "Inject IPython Client Connection Code",
    execute: async () => {
      const cluster = Private.clusterFromClick(app, sidebar.clusterManager);
      if (!cluster) {
        return;
      }
      return await clientCodeInjector(cluster);
    },
  });

  // Add a command to launch a new cluster.
  app.commands.addCommand(CommandIDs.newCluster, {
    label: (args) => (args["isPalette"] ? "Create New Cluster" : "NEW"),
    execute: () => sidebar.clusterManager.create(),
    iconClass: (args) =>
      args["isPalette"] ? "" : "jp-AddIcon jp-Icon jp-Icon-16",
    isEnabled: () => sidebar.clusterManager.isReady,
    caption: () => {
      if (sidebar.clusterManager.isReady) {
        return "Start New Cluster";
      }
      return "Cluster starting...";
    },
  });

  // Add a command to launch a new cluster.
  app.commands.addCommand(CommandIDs.startCluster, {
    label: "Start Cluster",
    execute: () => {
      const cluster = Private.clusterFromClick(app, sidebar.clusterManager);
      if (!cluster) {
        return;
      }
      return sidebar.clusterManager.start(cluster.id);
    },
  });

  // Add a command to stop a cluster.
  app.commands.addCommand(CommandIDs.stopCluster, {
    label: "Shutdown Cluster",
    execute: () => {
      const cluster = Private.clusterFromClick(app, sidebar.clusterManager);
      if (!cluster) {
        return;
      }
      return sidebar.clusterManager.stop(cluster.id);
    },
  });

  // Add a command to resize a cluster.
  app.commands.addCommand(CommandIDs.scaleCluster, {
    label: "Scale Cluster…",
    execute: () => {
      const cluster = Private.clusterFromClick(app, sidebar.clusterManager);
      if (!cluster) {
        return;
      }
      return sidebar.clusterManager.scale(cluster.id);
    },
  });

  // Add a command to toggle the auto-starting client code.
  // app.commands.addCommand(CommandIDs.toggleAutoStartClient, {
  //   label: "Auto-Start IPython Parallel",
  //   isToggled: () => autoStartClient,
  //   execute: async () => {
  //     const value = !autoStartClient;
  //     const key = "autoStartClient";
  //     return settingRegistry
  //       .set(PLUGIN_ID, key, value)
  //       .catch((reason: Error) => {
  //         console.error(
  //           `Failed to set ${PLUGIN_ID}:${key} - ${reason.message}`
  //         );
  //       });
  //   },
  // });

  // // Add some commands to the menu and command palette.
  // mainMenu.settingsMenu.addGroup([
  //   { command: CommandIDs.toggleAutoStartClient },
  // ]);
  // [CommandIDs.newCluster, CommandIDs.toggleAutoStartClient].forEach(
  //   (command) => {
  //     commandPalette.addItem({
  //       category: "IPython Parallel",
  //       command,
  //       args: { isPalette: true },
  //     });
  //   }
  // );

  // Add a context menu items.
  app.contextMenu.addItem({
    command: CommandIDs.injectClientCode,
    selector: ".ipp-ClusterListingItem",
    rank: 10,
  });
  app.contextMenu.addItem({
    command: CommandIDs.stopCluster,
    selector: ".ipp-ClusterListingItem",
    rank: 3,
  });
  app.contextMenu.addItem({
    command: CommandIDs.scaleCluster,
    selector: ".ipp-ClusterListingItem",
    rank: 2,
  });
  app.contextMenu.addItem({
    command: CommandIDs.startCluster,
    selector: ".ipp-ClusterListing-list",
    rank: 1,
  });
}

namespace Private {
  /**
   * A private counter for ids.
   */
  export let id = 0;

  /**
   * Whether a kernel should be used. Only evaluates to true
   * if it is valid and in python.
   */
  export async function shouldUseKernel(
    kernel: Kernel.IKernelConnection | null | undefined,
  ): Promise<boolean> {
    if (!kernel) {
      return false;
    }
    const spec = await kernel.spec;
    return !!spec && spec.language.toLowerCase().indexOf("python") !== -1;
  }

  /**
   * Connect a kernel to a cluster by creating a new Client.
   */
  export async function createClientForKernel(
    model: IClusterModel,
    kernel: Kernel.IKernelConnection,
  ): Promise<string> {
    const code = getClientCode(model);
    const content: KernelMessage.IExecuteRequestMsg["content"] = {
      store_history: false,
      code,
    };
    return new Promise<string>((resolve, _) => {
      const future = kernel.requestExecute(content);
      future.onIOPub = (msg) => {
        if (msg.header.msg_type !== "display_data") {
          return;
        }
        resolve(void 0);
      };
    });
  }

  /**
   * Insert code to connect to a given cluster.
   */
  export function injectClientCode(
    cluster: IClusterModel,
    editor: CodeEditor.IEditor,
  ): void {
    const cursor = editor.getCursorPosition();
    const offset = editor.getOffsetAt(cursor);
    const code = getClientCode(cluster);
    editor.model.sharedModel.updateSource(offset, offset, code);
  }

  /**
   * Get code to connect to a given cluster.
   */
  export function getClientCode(cluster: IClusterModel): string {
    return `import ipyparallel as ipp

cluster = ipp.Cluster.from_file("${cluster.cluster_file}")
rc = cluster.connect_client_sync()
rc`;
  }

  /**
   * Get the currently focused kernel in the application,
   * checking both notebooks and consoles.
   */
  export function getCurrentKernel(
    shell: ILabShell,
    notebookTracker: INotebookTracker,
    consoleTracker: IConsoleTracker,
  ): Kernel.IKernelConnection | null | undefined {
    // Get a handle on the most relevant kernel,
    // whether it is attached to a notebook or a console.
    let current = shell.currentWidget;
    let kernel: Kernel.IKernelConnection | null | undefined;
    if (current && notebookTracker.has(current)) {
      kernel = (current as NotebookPanel).sessionContext.session?.kernel;
    } else if (current && consoleTracker.has(current)) {
      kernel = (current as ConsolePanel).sessionContext.session?.kernel;
    } else if (notebookTracker.currentWidget) {
      const current = notebookTracker.currentWidget;
      kernel = current.sessionContext.session?.kernel;
    } else if (consoleTracker.currentWidget) {
      const current = consoleTracker.currentWidget;
      kernel = current.sessionContext.session?.kernel;
    }
    return kernel;
  }

  /**
   * Get the currently focused editor in the application,
   * checking both notebooks and consoles.
   * In the case of a notebook, it creates a new cell above the currently
   * active cell and then returns that.
   */
  export async function getCurrentEditor(
    app: JupyterFrontEnd,
    notebookTracker: INotebookTracker,
    consoleTracker: IConsoleTracker,
  ): Promise<CodeEditor.IEditor | null | undefined> {
    // Get a handle on the most relevant kernel,
    // whether it is attached to a notebook or a console.
    let current = app.shell.currentWidget;
    let editor: CodeEditor.IEditor | null | undefined;
    if (current && notebookTracker.has(current)) {
      NotebookActions.insertAbove((current as NotebookPanel).content);
      const cell = (current as NotebookPanel).content.activeCell;
      await cell.ready;
      editor = cell && cell.editor;
    } else if (current && consoleTracker.has(current)) {
      const cell = (current as ConsolePanel).console.promptCell;
      await cell.ready;
      editor = cell && cell.editor;
    } else if (notebookTracker.currentWidget) {
      const current = notebookTracker.currentWidget;
      NotebookActions.insertAbove(current.content);
      const cell = current.content.activeCell;
      await cell.ready;
      editor = cell && cell.editor;
    } else if (consoleTracker.currentWidget) {
      const current = consoleTracker.currentWidget;
      const cell = current.console.promptCell;
      await cell.ready;
      editor = cell && cell.editor;
    }
    return editor;
  }

  /**
   * Get a cluster model based on the application context menu click node.
   */
  export function clusterFromClick(
    app: JupyterFrontEnd,
    manager: ClusterManager,
  ): IClusterModel | undefined {
    const test = (node: HTMLElement) => !!node.dataset.clusterId;
    const node = app.contextMenuHitTest(test);
    if (!node) {
      return undefined;
    }
    const id = node.dataset.clusterId;

    return manager.clusters.find((cluster) => cluster.id === id);
  }
}
