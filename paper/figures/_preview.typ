// Preview one figure on its own crop page while drawing it:
//   just -f figures/justfile one fig_simd_dispatch
#import "style.typ": fig
#let name = sys.inputs.at("fig", default: "fig_simd_dispatch")
#show: fig
#if name == "fig_simd_dispatch" { import "fig_simd_dispatch.typ": panels; panels } else if name == "fig_mojibake_band" { import "fig_mojibake_band.typ": panels; panels } else if name == "fig_wildcopy" { import "fig_wildcopy.typ": panels; panels } else if name == "fig_fused_pipeline" { import "fig_fused_pipeline.typ": panels; panels } else if name == "fig_file_anatomy" { import "fig_file_anatomy.typ": panels; panels } else if name == "fig_schema_assembly" { import "fig_schema_assembly.typ": panels; panels } else if name == "fig_slotted_page" { import "fig_slotted_page.typ": panels; panels } else if name == "fig_throughput" { import "fig_throughput.typ": panels; panels } else if name == "fig_extent_tuning" { import "fig_extent_tuning.typ": panels; panels } else { panic("unknown figure: " + name) }
