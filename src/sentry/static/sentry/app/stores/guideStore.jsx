import Reflux from 'reflux';
import $ from 'jquery';
import GuideActions from '../actions/guideActions';
import HookStore from './hookStore';

const GuideStore = Reflux.createStore({
  init() {
    this.state = {
      // All guides returned to us from the server.
      guides: {},
      // We record guides seen on the server, but immediately after a user dismisses a guide
      // it may not have been synced yet, so the local copy helps in filtering correctly.
      guidesSeen: new Set(),
      // All anchors that have been registered on this current view.
      anchors: new Set(),
      // The "on deck" guide.
      currentGuide: null,
      // The current step of the current guide (1-indexed). 0 if there's no guide
      // or the guide is just cued but not opened.
      currentStep: 0,
    };
    this.listenTo(GuideActions.fetchSucceeded, this.onFetchSucceeded);
    this.listenTo(GuideActions.closeGuide, this.onCloseGuide);
    this.listenTo(GuideActions.nextStep, this.onNextStep);
    this.listenTo(GuideActions.registerAnchor, this.onRegisterAnchor);
    this.listenTo(GuideActions.unregisterAnchor, this.onUnregisterAnchor);
  },

  onFetchSucceeded(data) {
    this.state.guides = data;
    this.updateCurrentGuide();
  },

  onCloseGuide() {
    let {currentGuide, guidesSeen} = this.state;
    guidesSeen.add(currentGuide.id);
    this.updateCurrentGuide();
    HookStore.get('analytics:event').forEach(cb =>
      cb('assistant.guide_closed', {
        guide: currentGuide.id,
        cue: currentGuide.cue,
      })
    );
  },

  onNextStep() {
    this.state.currentStep += 1;
    this.trigger(this.state);
    HookStore.get('analytics:event').forEach(cb =>
      cb('assistant.guide_next', {
        guide: this.state.currentGuide.id,
        cue: this.state.currentGuide.cue,
      })
    );
  },

  onRegisterAnchor(anchor) {
    this.state.anchors.add(anchor);
    this.updateCurrentGuide();
  },

  onUnregisterAnchor(anchor) {
    this.state.anchors.delete(anchor);
    this.updateCurrentGuide();
  },

  updateCurrentGuide() {
    let availableTargets = [...this.state.anchors].map(a => a.props.target);

    // Select the first guide that hasn't been seen in this session and has all
    // required anchors on the page.
    let bestGuideKey = Object.keys(this.state.guides).find(key => {
      let guide = this.state.guides[key];
      let seen = this.state.guidesSeen.has(guide.id);
      let allTargetsPresent = guide.required_targets.every(
        t => availableTargets.indexOf(t) >= 0
      );
      return !seen && allTargetsPresent;
    });

    let bestGuide = null;
    if (bestGuideKey) {
      bestGuide = $.extend(true, {}, this.state.guides[bestGuideKey]);
      // Remove steps that don't have an anchor on the page.
      bestGuide.steps = bestGuide.steps.filter(
        step => step.target && availableTargets.indexOf(step.target) >= 0
      );
    }

    if (bestGuide !== this.state.currentGuide) {
      this.state.currentGuide = bestGuide;
      this.state.currentStep = 0;
      this.trigger(this.state);
    }
  },
});

export default GuideStore;
