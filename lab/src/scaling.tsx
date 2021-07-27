import { Dialog, showDialog } from "@jupyterlab/apputils";

import { IClusterModel } from "./clusters";
import * as React from "react";

/**
 * A namespace for ClusterScaling statics.
 */
namespace ClusterScaling {
  /**
   * The props for the ClusterScaling component.
   */
  export interface IProps {
    /**
     * The initial cluster model shown in the scaling.
     */
    initialModel: IClusterModel;

    /**
     * A callback that allows the component to write state to an
     * external object.
     */
    stateEscapeHatch: (model: IClusterModel) => void;
  }

  /**
   * The state for the ClusterScaling component.
   */
  export interface IState {
    /**
     * The proposed cluster model shown in the scaling.
     */
    model: IClusterModel;
  }
}

/**
 * A component for an HTML form that allows the user
 * to select scaling parameters.
 */
export class ClusterScaling extends React.Component<
  ClusterScaling.IProps,
  ClusterScaling.IState
> {
  /**
   * Construct a new ClusterScaling component.
   */
  constructor(props: ClusterScaling.IProps) {
    super(props);
    let model: IClusterModel;
    model = props.initialModel;

    this.state = { model };
  }

  /**
   * When the component updates we take the opportunity to write
   * the state of the cluster to an external object so this can
   * be sent as the result of the dialog.
   */
  componentDidUpdate(): void {
    let model: IClusterModel = { ...this.state.model };
    this.props.stateEscapeHatch(model);
  }

  /**
   * React to the number of workers changing.
   */
  onScaleChanged(event: React.ChangeEvent): void {
    this.setState({
      model: {
        ...this.state.model,
        workers: parseInt((event.target as HTMLInputElement).value, 10),
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
        <span className="ipp-ScalingHeader">Manual Scaling</span>
        <div className="ipp-ScalingSection">
          <div className="ipp-ScalingSection-item">
            <span className={`ipp-ScalingSection-label`}>Engines</span>
            <input
              className="ipp-ScalingInput"
              value={model.engines}
              type="number"
              step="1"
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
 * Show a dialog for scaling a cluster model.
 *
 * @param model: the initial model.
 *
 * @returns a promse that resolves with the user-selected scalings for the
 *   cluster model. If they pressed the cancel button, it resolves with
 *   the original model.
 */
export function showScalingDialog(
  model: IClusterModel
): Promise<IClusterModel> {
  let updatedModel = { ...model };
  const escapeHatch = (update: IClusterModel) => {
    updatedModel = update;
  };

  return showDialog({
    title: `Scale ${model.name}`,
    body: (
      <ClusterScaling initialModel={model} stateEscapeHatch={escapeHatch} />
    ),
    buttons: [Dialog.cancelButton(), Dialog.okButton({ label: "SCALE" })],
  }).then((result) => {
    if (result.button.accept) {
      return updatedModel;
    } else {
      return model;
    }
  });
}
