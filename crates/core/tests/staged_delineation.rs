use geo::BoundingRect;
use hfx_core::Level;
use shed_core::algo::coord::GeoCoord;
use shed_core::session::DatasetSession;
use shed_core::testutil::DatasetBuilder;
use shed_core::{Engine, LevelSelection};

#[test]
fn staged_level_selection_parses_finest_before_resolution() {
    let (_dir, root) = DatasetBuilder::new(1).with_multilevel_nested().build();
    let session = DatasetSession::open_path(&root).expect("nested fixture should open");
    let engine = Engine::builder(session).build();

    let selected = engine
        .select_level(LevelSelection::Finest)
        .expect("finest level should resolve");

    assert_eq!(selected.level(), Level::new(1).expect("fixture level"));
}

#[test]
fn staged_pre_merge_units_are_pristine_terminal_first_records() {
    let (_dir, root) = DatasetBuilder::new(1).with_multilevel_nested().build();
    let session = DatasetSession::open_path(&root).expect("nested fixture should open");
    let engine = Engine::builder(session).build();
    let selected = engine
        .select_level(LevelSelection::Finest)
        .expect("finest level should resolve");
    let resolved = engine
        .resolve_outlet_at_level(GeoCoord::new(2.5, -0.5), selected, &Default::default())
        .expect("fixture outlet should resolve to terminal L1 unit");
    let upstream = engine
        .traverse_upstream_at_level(&resolved)
        .expect("same-level traversal should succeed");

    let pre_merge = engine
        .produce_pre_merge_units(&upstream)
        .expect("pre-merge units should materialize");

    assert_eq!(pre_merge.selected_level(), selected);
    assert_eq!(pre_merge.terminal(), resolved.resolved().unit_id);
    assert_eq!(pre_merge.units().len(), 3);
    assert_eq!(
        pre_merge.units()[0].id(),
        resolved.resolved().unit_id,
        "terminal must be first for typed inspection"
    );
    assert_eq!(
        pre_merge.terminal_unit().map(|unit| unit.id()),
        Some(pre_merge.terminal())
    );

    let terminal = pre_merge
        .terminal_unit()
        .expect("terminal record should be available");
    let bbox = terminal
        .geometry()
        .bounding_rect()
        .expect("fixture terminal geometry should have a bbox");
    assert_eq!(bbox.min().x, 2.0);
    assert_eq!(bbox.min().y, -1.0);
    assert_eq!(bbox.max().x, 3.0);
    assert_eq!(bbox.max().y, 0.0);

    for unit in pre_merge.units() {
        assert_eq!(
            unit.level(),
            selected.level(),
            "all pre-merge records must stay at SelectedLevel"
        );
        assert_eq!(unit.area().get(), 10.0);
        assert!(unit.outlet().lon().is_finite());
        assert!(unit.outlet().lat().is_finite());
        assert_eq!(unit.up_area(), None);
        assert!(
            unit.geometry().bounding_rect().is_some(),
            "every record must include decoded whole geometry"
        );
    }
}
