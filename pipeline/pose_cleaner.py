import numpy as np
import pandas as pd
import cv2
import matplotlib.pyplot as plt
import os

def get_columns(df, search_str, method='contains', exclude=None):
    if method == 'startswith':
        cols = df.columns[df.columns.str.startswith(search_str)]
    elif method == 'contains':
        cols = df.columns[df.columns.str.contains(search_str)]
    else:
        raise ValueError("method must be 'startswith' or 'contains'")
    
    if exclude is not None:
        if isinstance(exclude, str):
            exclude = [exclude]
        for word in exclude:
            cols = [c for c in cols if word not in c]

    df_cols = df[cols]
    return df_cols

class PoseCleaner:
    def __init__(self, loader, verbose=True):
        self.loader = loader
        self.verbose = verbose

    def compute_diffs(self, p):
        return np.vstack([[0, 0], np.diff(p, axis=0)])
    
    def group_close_points(self, close_points, max_gap=50):
        """
        Groups consecutive indices if they are within `max_gap` apart.
        Returns a list of index groups.
        """
        if close_points.size == 1:
            return [[close_points, close_points]]
        if len(close_points) == 0:
            return []
        
        groups = [[close_points[0]]]
        for idx in close_points[1:]:
            if idx - groups[-1][-1] <= max_gap:
                groups[-1].append(idx)
            else:
                groups.append([idx])
        return groups
    
    def get_same_diff_groups(self, pos, n_same=20, max_gap = 10, tol = 1e-5):
        diffs = np.diff(pos, axis=0)

        same_diff = np.linalg.norm(np.diff(diffs, axis=0), axis=1) < tol
        same_diff = np.concatenate(([False], same_diff, [False]))

        N = n_same 
        mask = np.zeros_like(same_diff, dtype=bool)

        count = 0
        for i, val in enumerate(same_diff):
            if val:
                count += 1
            else:
                if count >= N:
                    mask[i - count:i] = True
                count = 0
        same_diff = mask

        same_diff_groups = self.group_close_points(np.argwhere(same_diff).squeeze(), max_gap=max_gap)
        return same_diff_groups

    def read_frame(self, cap, frame_idx):
        cap.set(cv2.CAP_PROP_POS_FRAMES, int(frame_idx))
        ret, frame = cap.read()
        if not ret:
            return None
        return cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    
    def plot_groups_w_video_combiner(self, groups, positions, video_path, plotlen=30, max_plots = 10, random=True):
        
        if isinstance(positions, np.ndarray):
            positions = [positions]

        cap = cv2.VideoCapture(video_path)
        if random:
            np.random.shuffle(groups)

        rows, cols = int(np.ceil(max_plots/3)), 3
        fig, axes = plt.subplots(rows, cols, figsize=(20*cols, 20*rows))
        axes = axes.flatten()

        for i, group in enumerate(groups):
            if i == max_plots:
                break
            
            start = max(0, group[0] - plotlen)
            end = min(len(positions[0]), group[-1] + plotlen)

            start_frame = self.read_frame(cap, start)
            end_frame   = self.read_frame(cap, end)

            axes[i].imshow(start_frame)
            axes[i].imshow(end_frame, alpha=0.5)   
            for position in positions:
                axes[i].plot(*position[start:end].T, lw=1)
            axes[i].axis("off")
        
        fig.savefig(os.path.join(self.loader.output_path, 'pose_cleaner_QC.png'))
        plt.close(fig)
        cap.release()

    def corner_row_mask_df(self, df, x_suffix=".x", y_suffix=".y"):
        # collect x/y columns in matching order
        x_cols = [c for c in df.columns if c.endswith(x_suffix)]
        y_cols = [c for c in df.columns if c.endswith(y_suffix)]

        # ensure we only keep pairs that exist in both
        x_base = {c[:-len(x_suffix)] for c in x_cols}
        y_base = {c[:-len(y_suffix)] for c in y_cols}
        bases = sorted(x_base & y_base)

        x_cols = [b + x_suffix for b in bases]
        y_cols = [b + y_suffix for b in bases]

        X = df[x_cols].to_numpy(dtype=float) 
        Y = df[y_cols].to_numpy(dtype=float)

        left   = 200
        right  = 2100
        bottom = 200
        top    = 1800

        in_corner = (
            ((X < left)  & (Y < bottom)) |
            ((X < left)  & (Y > top))    |
            ((X > right) & (Y < bottom)) |
            ((X > right) & (Y > top))
        )  

        return in_corner.any(axis=1)

    def get_all_diffs_df(self, df, keypoints):
        diffs = df.copy()
        for kp in keypoints:
            pos_kp = get_columns(df, kp, 'contains', exclude='score')
            new_df = (pos_kp.bfill()
                        .diff()
                        .mask(pos_kp.shift().isna())
                        .ffill()
                    )
            diffs[f'{kp}.diff'] = np.linalg.norm(new_df.values,axis=1)
        return diffs
    
    def get_jumps_and_unsmooth(self, full_df, keypoints, smooth_window = 3, max_idx_gap = 10, max_idx_gap_smooth = 5, jump_threshold=200, smooth_diff_threshold=25):
        
        jump_groups_dict = {}
        smooth_jump_groups_dict = {}

        for kp_n, kp in enumerate(keypoints):
            # find jumps, write them to dict and set a column for them
            jumps = full_df[f'{kp}.diff'] > jump_threshold 
            jump_groups = self.group_close_points(np.where(jumps.values)[0], max_gap=max_idx_gap)
            jump_groups_dict[kp] = jump_groups
            jumps = [idx for group in jump_groups for idx in group]
            full_df[f'{kp}.jumps'] = 0
            full_df.iloc[jumps, full_df.columns.get_loc(f'{kp}.jumps')] = 1

            # remove the jumps and interpolate them
            full_df[[f'{kp}_clean.x', f'{kp}_clean.y']] = full_df[[f'{kp}.x', f'{kp}.y']].copy().mask(full_df[f'{kp}.jumps'] == 1) 
            full_df[[f'{kp}_clean.x', f'{kp}_clean.y']] = full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].interpolate(method='time', limit_direction='both')

            # now compare smoothness and normal, find where they differ significantly, write to dict and set column
            full_df[[f'{kp}_clean_smooth.x', f'{kp}_clean_smooth.y']] = full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].copy().rolling(window=smooth_window, win_type='gaussian', center=True).mean(std=3)
            diff_between_normal_and_smooth = np.linalg.norm(full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].values - full_df[[f'{kp}_clean_smooth.x', f'{kp}_clean_smooth.y']].values, axis=1)
            full_df[f'{kp}_clean_smooth.diff'] = diff_between_normal_and_smooth
            smooth_jumps = full_df[f'{kp}_clean_smooth.diff'] > smooth_diff_threshold
            smooth_jump_groups = self.group_close_points(np.where(smooth_jumps.values)[0], max_gap=max_idx_gap_smooth)
            smooth_jump_groups_dict[kp] = smooth_jump_groups
            smooth_jumps = [idx for group in smooth_jump_groups for idx in group]
            full_df[f'{kp}_clean_smooth_jump'] = 0
            full_df.iloc[smooth_jumps, full_df.columns.get_loc(f'{kp}_clean_smooth_jump')] = 1

            # remove the smooth jumps and interpolate them
            full_df[[f'{kp}_clean2.x', f'{kp}_clean2.y']] = full_df[[f'{kp}_clean.x', f'{kp}_clean.y']].copy().mask(full_df[f'{kp}_clean_smooth_jump'] == 1)
            full_df[[f'{kp}_clean2.x', f'{kp}_clean2.y']] = full_df[[f'{kp}_clean2.x', f'{kp}_clean2.y']].interpolate(method='time', limit_direction='both')

            # remove all the intermediate stuff, rename the clean columns to the original names + clean
            full_df.drop(columns=[f'{kp}.diff', f'{kp}.jumps', f'{kp}_clean.x', f'{kp}_clean.y', f'{kp}_clean_smooth.x', f'{kp}_clean_smooth.y', f'{kp}_clean_smooth.diff', f'{kp}_clean_smooth_jump'], inplace=True)
            full_df.columns = [c.replace(f'{kp}_clean2', f'{kp}_clean') for c in full_df.columns]
            
        return full_df, jump_groups_dict, smooth_jump_groups_dict

    def plot_orig_clean_and_jumps(self, init_kp, clean_kp, jump_groups_dict, smooth_jump_groups_dict, plot_kp):
        fig, ax = plt.subplots(1,3, figsize=(30,10))
        ax[0].plot(*init_kp.T,lw=.3)
        ax[0].set_title("Original")
        ax[1].plot(*clean_kp.T,lw=.3)
        ax[1].set_title("Cleaned")
        # plot the nans_in_plot_kp as separate segments instead of all connected
        for group in smooth_jump_groups_dict[plot_kp]:
            ax[2].plot(*init_kp[group].T, lw = .3)
        for group in jump_groups_dict[plot_kp]:
            ax[2].plot(*init_kp[group].T, lw = .3)
        ax[2].set_title("stuf taken out")
        fig.savefig(os.path.join(self.loader.output_path, 'pose_cleaner_QC_jumps.png'))
        plt.close(fig)

    def clean_dataframe(self, df, video_path, plot):

        sleap_pose = get_columns(df, '','contains', exclude=['score','centroid']).copy() 
        if sleap_pose.empty:
            print("No pose columns found in the dataframe.")
            return df
        
        base_keypoint = 'pose_tailstart1'
        
        xy_cols = sleap_pose.columns
        keypoints = np.unique([c[:-2] for c in xy_cols])
        
        nans0 = self.corner_row_mask_df(sleap_pose)

        df_clean = df.copy()
        df_clean.loc[nans0, xy_cols] = np.nan 
        
        full_df = self.get_all_diffs_df(df_clean, keypoints)
        full_df, jump_groups_dict, smooth_jump_groups_dict = self.get_jumps_and_unsmooth(full_df, keypoints, jump_threshold=200, smooth_diff_threshold=30)

        init_kp = get_columns(sleap_pose, base_keypoint, 'contains', exclude='score').values
        clean_kp = get_columns(full_df, base_keypoint+'_clean', 'contains', exclude='score').values

        big_jumps = jump_groups_dict[base_keypoint]
        smooth_jumps = smooth_jump_groups_dict[base_keypoint]
        
        if plot and sum(len(g) for g in big_jumps + smooth_jumps) > 15:
            self.plot_groups_w_video_combiner(big_jumps + smooth_jumps, [init_kp, clean_kp], video_path, plotlen=15, max_plots=9, random=True) 
            self.plot_orig_clean_and_jumps(init_kp, clean_kp, jump_groups_dict, smooth_jump_groups_dict, base_keypoint)
        else:
            print('data was clean, no bad stuff to plot :) ') if self.verbose else None
        return full_df