import { Dialog, showDialog } from "@jupyterlab/apputils";

import * as React from "react";

export interface INewCluster {
  /**
   * The cluster id
   */
  cluster_id?: string;
  /**
   * The profile
   */
  profile?: string;
  /**
   * The number of engines
   */
  n?: number;
}

/**
 * A namespace for ClusterDialog statics.
 */
namespace NewCluster {
  /**
   * The props for the NewClusterDialog component.
   */

  export interface IProps {
    /**
     * The initial cluster model shown in the Dialog.
     */
    initialModel: INewCluster;

    /**
     * A callback that allows the component to write state to an
     * external object.
     */
    stateEscapeHatch: (model: INewCluster) => void;
  }

  /**
   * The state for the ClusterDialog component.
   */
  export interface IState {
    /**
     * The proposed cluster model shown in the Dialog.
     */
    model: INewCluster;
  }
}

/**
 * A component for an HTML form that allows the user
 * to select Dialog parameters.
 */
export class NewCluster extends React.Component<
  NewCluster.IProps,
  NewCluster.IState
> {
  /**
   * Construct a new NewCluster component.
   */
  constructor(props: NewCluster.IProps) {
    super(props);
    let model: INewCluster;
    model = props.initialModel;

    this.state = { model };
  }

  /**
   * When the component updates we take the opportunity to write
   * the state of the cluster to an external object so this can
   * be sent as the result of the dialog.
   */
  componentDidUpdate(): void {
    let model: INewCluster = { ...this.state.model };
    this.props.stateEscapeHatch(model);
  }

  /**
   * React to the number of workers changing.
   */
  onScaleChanged(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        n: parseInt((event.target as HTMLInputElement).value || null, null),
      },
    });
  }

  /**
   * React to the number of workers changing.
   */
  onProfileChanged(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        profile: (event.target as HTMLInputElement).value,
      },
    });
  }

  /**
   * React to the number of workers changing.
   */
  onClusterIdChanged(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        cluster_id: (event.target as HTMLInputElement).value,
      },
    });
  }

  /**
   * Render the component..
   */
  render() {
    const model = this.state.model;
    // const disabledClass = "ipp-mod-disabled";
    return (
      <div>
        <div className="ipp-DialogSection">
          <div className="ipp-DialogSection-item">
            <span className={`ipp-DialogSection-label`}>Profile</span>
            <input
              className="ipp-DialogInput"
              value={model.profile}
              type="string"
              placeholder="default"
              onChange={(evt) => {
                this.onProfileChanged(evt);
              }}
            />
          </div>
          <div className="ipp-DialogSection-item">
            <span className={`ipp-DialogSection-label`}>Cluster ID</span>
            <input
              className="ipp-DialogInput"
              value={model.cluster_id}
              type="string"
              placeholder="auto"
              onChange={(evt) => {
                this.onClusterIdChanged(evt);
              }}
            />
          </div>
          <div className="ipp-DialogSection-item">
            <span className={`ipp-DialogSection-label`}>Engines</span>
            <input
              className="ipp-DialogInput"
              value={model.n}
              type="number"
              step="1"
              placeholder="auto"
              onChange={(evt) => {
                this.onScaleChanged(evt);
              }}
            />
          </div>
        </div>
      </div>
    );
  }
}

/**
 * Show a dialog for Dialog a cluster model.
 *
 * @param model: the initial model.
 *
 * @returns a promse that resolves with the user-selected Dialogs for the
 *   cluster model. If they pressed the cancel button, it resolves with
 *   the original model.
 */
export function newClusterDialog(model: INewCluster): Promise<INewCluster> {
  let updatedModel = { ...model };
  const escapeHatch = (update: INewCluster) => {
    updatedModel = update;
  };

  return showDialog({
    title: `New Cluster`,
    body: <NewCluster initialModel={model} stateEscapeHatch={escapeHatch} />,
    buttons: [Dialog.cancelButton(), Dialog.okButton({ label: "CREATE" })],
  }).then((result) => {
    if (result.button.accept) {
      return updatedModel;
    } else {
      return null;
    }
  });
}
