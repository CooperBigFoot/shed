use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use geo::{LineString, Polygon};
use shed_core::algo::dissolve;

fn rect(x0: f64, y0: f64, x1: f64, y1: f64) -> Polygon<f64> {
    Polygon::new(
        LineString::from(vec![(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]),
        vec![],
    )
}

fn disjoint_grid_fixture() -> Vec<Polygon<f64>> {
    (0..10)
        .flat_map(|row| {
            (0..10).map(move |col| {
                let x = f64::from(col) * 2.0;
                let y = f64::from(row) * 2.0;
                rect(x, y, x + 1.0, y + 1.0)
            })
        })
        .collect()
}

fn overlapping_chain_fixture() -> Vec<Polygon<f64>> {
    (0..100)
        .map(|i| {
            let x = f64::from(i) * 0.75;
            rect(x, 0.0, x + 1.0, 1.0)
        })
        .collect()
}

fn zurich_like_fixture() -> Vec<Polygon<f64>> {
    vec![
        rect(0.0, 0.0, 1.0, 1.0),
        rect(1.0, 0.0, 2.0, 1.0),
        rect(2.0, 0.0, 3.0, 1.0),
        rect(0.0, 1.0, 1.0, 2.0),
        rect(1.0, 1.0, 2.0, 2.0),
        rect(2.0, 1.0, 3.0, 2.0),
        rect(3.0, 1.0, 4.0, 2.0),
        rect(1.0, 2.0, 2.0, 3.0),
        rect(2.0, 2.0, 3.0, 3.0),
        rect(3.0, 2.0, 4.0, 3.0),
        rect(4.0, 2.0, 5.0, 3.0),
        rect(2.0, 3.0, 3.0, 4.0),
        rect(3.0, 3.0, 4.0, 4.0),
        rect(4.0, 3.0, 5.0, 4.0),
        rect(5.0, 3.0, 6.0, 4.0),
        rect(3.0, 4.0, 4.0, 5.0),
        rect(4.0, 4.0, 5.0, 5.0),
        rect(5.0, 4.0, 6.0, 5.0),
        rect(4.0, 5.0, 5.0, 6.0),
        rect(5.0, 5.0, 6.0, 6.0),
    ]
}

fn bench_dissolve_strategies(c: &mut Criterion) {
    let fixtures = [
        ("disjoint_grid", disjoint_grid_fixture()),
        ("overlapping_chain", overlapping_chain_fixture()),
        ("zurich_like", zurich_like_fixture()),
    ];

    let mut group = c.benchmark_group("dissolve_strategy");
    for (name, polygons) in fixtures {
        group.bench_with_input(BenchmarkId::new("reduce", name), &polygons, |b, input| {
            b.iter(|| dissolve::dissolve_reduce_strategy(input.clone()))
        });
        group.bench_with_input(
            BenchmarkId::new("spatial_reduce", name),
            &polygons,
            |b, input| b.iter(|| dissolve::dissolve_spatial_reduce_strategy(input.clone())),
        );
        group.bench_with_input(
            BenchmarkId::new("unary_union", name),
            &polygons,
            |b, input| b.iter(|| dissolve::dissolve_unary_union_strategy(input.clone())),
        );
        group.bench_with_input(
            BenchmarkId::new("raw_unary_union_comparison", name),
            &polygons,
            |b, input| b.iter(|| dissolve::dissolve_raw_unary_union_comparison(input.clone())),
        );
    }
    group.finish();
}

criterion_group!(benches, bench_dissolve_strategies);
criterion_main!(benches);
