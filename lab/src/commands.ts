export namespace CommandIDs {
  /**
   * Inject client code into the active editor.
   */
  export const injectClientCode = "ipyparallel:inject-client-code";

  /**
   * Launch a new cluster.
   */
  export const newCluster = "ipyparallel:new-cluster";

  /**
   * Launch a new cluster.
   */
  export const startCluster = "ipyparallel:start-cluster";

  /**
   * Shutdown a cluster.
   */
  export const stopCluster = "ipyparallel:stop-cluster";

  /**
   * Scale a cluster.
   */
  export const scaleCluster = "ipyparallel:scale-cluster";

  /**
   * Toggle the auto-starting of clients.
   */
  export const toggleAutoStartClient = "ipyparallel:toggle-auto-start-client";
}
