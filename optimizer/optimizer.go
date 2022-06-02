package optimizer

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func OptimizeQuery(ql string) string {
	expr, err := parser.ParseExpr(ql)
	if err != nil {
		panic(err)
	}

	return OptimizeExpr(expr).String()
}

func OptimizeExpr(expr parser.Expr) parser.Expr {
	clone := Clone(expr)
	optimizeInplace(clone)
	return clone
}

// Clone clones the given expression e and returns the cloned copy.
func Clone(e parser.Expr) parser.Expr {
	s := e.String()
	eCopy, err := parser.ParseExpr(s)
	if err != nil {
		panic(fmt.Errorf("BUG: cannot parse the expression %q: %w", s, err))
	}
	return eCopy
}

func optimizeInplace(e parser.Expr) {
	switch t := e.(type) {
	// case *parser.RollupExpr:
	// 	optimizeInplace(t.Expr)
	// 	optimizeInplace(t.At)
	case *parser.Call:
		for _, arg := range t.Args {
			optimizeInplace(arg)
		}
	case *parser.AggregateExpr:
		optimizeInplace(t.Param)
	case *parser.BinaryExpr:
		optimizeInplace(t.LHS)
		optimizeInplace(t.RHS)
		lfs := getCommonLabelFilters(t)
		pushdownBinaryOpFiltersInplace(t, lfs)
	}
}

func getCommonLabelFilters(e parser.Expr) []*labels.Matcher {
	switch t := e.(type) {
	case *parser.VectorSelector:
		return getLabelFiltersWithoutMetricName(t.LabelMatchers)
	case *parser.SubqueryExpr:
		return getCommonLabelFilters(t.Expr)
	case *parser.Call:
		arg := getFuncArgForOptimization(t.Func.Name, t.Args...)
		if arg == nil {
			return nil
		}
		return getCommonLabelFilters(arg)
	case *parser.AggregateExpr:
		arg := getFuncArgForOptimization(t.Op.String(), t.Param)
		if arg == nil {
			return nil
		}
		lfs := getCommonLabelFilters(arg)
		return trimFiltersByAggrModifier(lfs, t)
	case *parser.BinaryExpr:
		lfsLeft := getCommonLabelFilters(t.LHS)
		lfsRight := getCommonLabelFilters(t.RHS)
		var lfs []*labels.Matcher
		switch strings.ToLower(t.Op.String()) {
		case "or":
			// {fCommon, f1} or {fCommon, f2} -> {fCommon}
			// {fCommon, f1} or on() {fCommon, f2} -> {}
			// {fCommon, f1} or on(fCommon) {fCommon, f2} -> {fCommon}
			// {fCommon, f1} or on(f1) {fCommon, f2} -> {}
			// {fCommon, f1} or on(f2) {fCommon, f2} -> {}
			// {fCommon, f1} or on(f3) {fCommon, f2} -> {}
			lfs = intersectLabelFilters(lfsLeft, lfsRight)
			return TrimFiltersByGroupModifier(lfs, t)
		case "unless":
			// {f1} unless {f2} -> {f1}
			// {f1} unless on() {f2} -> {}
			// {f1} unless on(f1) {f2} -> {f1}
			// {f1} unless on(f2) {f2} -> {}
			// {f1} unless on(f1, f2) {f2} -> {f1}
			// {f1} unless on(f3) {f2} -> {}
			return TrimFiltersByGroupModifier(lfsLeft, t)
		default:
			switch strings.ToLower(t.VectorMatching.Card.String()) {
			case "one_to_many":
				// {f1} * group_left() {f2} -> {f1, f2}
				// {f1} * on() group_left() {f2} -> {f1}
				// {f1} * on(f1) group_left() {f2} -> {f1}
				// {f1} * on(f2) group_left() {f2} -> {f1, f2}
				// {f1} * on(f1, f2) group_left() {f2} -> {f1, f2}
				// {f1} * on(f3) group_left() {f2} -> {f1}
				lfsRight = TrimFiltersByGroupModifier(lfsRight, t)
				return unionLabelFilters(lfsLeft, lfsRight)
			case "many_to_one":
				// {f1} * group_right() {f2} -> {f1, f2}
				// {f1} * on() group_right() {f2} -> {f2}
				// {f1} * on(f1) group_right() {f2} -> {f1, f2}
				// {f1} * on(f2) group_right() {f2} -> {f2}
				// {f1} * on(f1, f2) group_right() {f2} -> {f1, f2}
				// {f1} * on(f3) group_right() {f2} -> {f2}
				lfsLeft = TrimFiltersByGroupModifier(lfsLeft, t)
				return unionLabelFilters(lfsLeft, lfsRight)
			default:
				// {f1} * {f2} -> {f1, f2}
				// {f1} * on() {f2} -> {}
				// {f1} * on(f1) {f2} -> {f1}
				// {f1} * on(f2) {f2} -> {f2}
				// {f1} * on(f1, f2) {f2} -> {f2}
				// {f1} * on(f3} {f2} -> {}
				lfs = unionLabelFilters(lfsLeft, lfsRight)
				return TrimFiltersByGroupModifier(lfs, t)
			}
		}
	default:
		return nil
	}
}

func trimFiltersByAggrModifier(lfs []*labels.Matcher, afe *parser.AggregateExpr) []*labels.Matcher {
	switch afe.Without {
	case false:
		return filterLabelFiltersOn(lfs, afe.Grouping)
	case true:
		return filterLabelFiltersIgnoring(lfs, afe.Grouping)
	default:
		return nil
	}
}

// TrimFiltersByGroupModifier trims lfs by the specified be.GroupModifier.Op (e.g. on() or ignoring()).
//
// The following cases are possible:
// - It returns lfs as is if be doesn't contain any group modifier
// - It returns only filters specified in on()
// - It drops filters specified inside ignoring()
func TrimFiltersByGroupModifier(lfs []*labels.Matcher, be *parser.BinaryExpr) []*labels.Matcher {
	switch be.VectorMatching.On {
	case true:
		return filterLabelFiltersOn(lfs, be.VectorMatching.Include)
	case false:
		return filterLabelFiltersIgnoring(lfs, be.VectorMatching.Include)
	default:
		return lfs
	}
}

func getLabelFiltersWithoutMetricName(lfs []*labels.Matcher) []*labels.Matcher {
	lfsNew := make([]*labels.Matcher, 0, len(lfs))
	for _, lf := range lfs {
		if lf.Name != "__name__" {
			lfsNew = append(lfsNew, lf)
		}
	}
	return lfsNew
}

// PushdownBinaryOpFilters pushes down the given commonFilters to e if possible.
//
// e must be a part of binary operation - either left or right.
//
// For example, if e contains `foo + sum(bar)` and commonFilters={x="y"},
// then the returned expression will contain `foo{x="y"} + sum(bar)`.
// The `{x="y"}` cannot be pusehd down to `sum(bar)`, since this may change binary operation results.
func PushdownBinaryOpFilters(e parser.Expr, commonFilters []*labels.Matcher) parser.Expr {
	if len(commonFilters) == 0 {
		// Fast path - nothing to push down.
		return e
	}
	eCopy := Clone(e)
	pushdownBinaryOpFiltersInplace(eCopy, commonFilters)
	return eCopy
}

func pushdownBinaryOpFiltersInplace(e parser.Expr, lfs []*labels.Matcher) {
	if len(lfs) == 0 {
		return
	}
	switch t := e.(type) {
	case *parser.VectorSelector:
		t.LabelMatchers = unionLabelFilters(t.LabelMatchers, lfs)
		sortLabelFilters(t.LabelMatchers)
	// case *RollupExpr:
	// 	pushdownBinaryOpFiltersInplace(t.Expr, lfs)
	case *parser.Call:
		arg := getFuncArgForOptimization(t.Func.Name, t.Args...)
		if arg != nil {
			pushdownBinaryOpFiltersInplace(arg, lfs)
		}
	case *parser.AggregateExpr:
		lfs = trimFiltersByAggrModifier(lfs, t)
		arg := getFuncArgForOptimization(t.Op.String(), t.Param)
		if arg != nil {
			pushdownBinaryOpFiltersInplace(arg, lfs)
		}
	case *parser.BinaryExpr:
		lfs = TrimFiltersByGroupModifier(lfs, t)
		pushdownBinaryOpFiltersInplace(t.LHS, lfs)
		pushdownBinaryOpFiltersInplace(t.RHS, lfs)
	}
}

func intersectLabelFilters(lfsA, lfsB []*labels.Matcher) []*labels.Matcher {
	if len(lfsA) == 0 || len(lfsB) == 0 {
		return nil
	}
	m := getLabelFiltersMap(lfsA)
	var lfs []*labels.Matcher
	for _, lf := range lfsB {
		if _, ok := m[lf.String()]; ok {
			lfs = append(lfs, lf)
		}
	}
	return lfs
}

func unionLabelFilters(lfsA, lfsB []*labels.Matcher) []*labels.Matcher {
	if len(lfsA) == 0 {
		return lfsB
	}
	if len(lfsB) == 0 {
		return lfsA
	}
	m := getLabelFiltersMap(lfsA)
	lfs := append([]*labels.Matcher{}, lfsA...)
	for _, lf := range lfsB {
		if _, ok := m[lf.String()]; !ok {
			lfs = append(lfs, lf)
		}
	}
	return lfs
}

func getLabelFiltersMap(lfs []*labels.Matcher) map[string]struct{} {
	m := make(map[string]struct{}, len(lfs))
	for _, lf := range lfs {
		m[lf.String()] = struct{}{}
	}
	return m
}

func sortLabelFilters(lfs []*labels.Matcher) {
	// Make sure the first label filter is __name__ (if any)
	if len(lfs) > 0 && lfs[0].Name == "__name__" {
		lfs = lfs[1:]
	}
	sort.Slice(lfs, func(i, j int) bool {
		a, b := lfs[i], lfs[j]
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.Value < b.Value
	})
}

func filterLabelFiltersOn(lfs []*labels.Matcher, args []string) []*labels.Matcher {
	if len(args) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(args))
	for _, arg := range args {
		m[arg] = struct{}{}
	}
	var lfsNew []*labels.Matcher
	for _, lf := range lfs {
		if _, ok := m[lf.Name]; ok {
			lfsNew = append(lfsNew, lf)
		}
	}
	return lfsNew
}

func filterLabelFiltersIgnoring(lfs []*labels.Matcher, args []string) []*labels.Matcher {
	if len(args) == 0 {
		return lfs
	}
	m := make(map[string]struct{}, len(args))
	for _, arg := range args {
		m[arg] = struct{}{}
	}
	var lfsNew []*labels.Matcher
	for _, lf := range lfs {
		if _, ok := m[lf.Name]; !ok {
			lfsNew = append(lfsNew, lf)
		}
	}
	return lfsNew
}

func getFuncArgForOptimization(funcName string, args ...parser.Expr) parser.Expr {
	idx := getFuncArgIdxForOptimization(funcName, args...)
	if idx < 0 || idx >= len(args) {
		return nil
	}
	return args[idx]
}

func getFuncArgIdxForOptimization(funcName string, args ...parser.Expr) int {
	funcName = strings.ToLower(funcName)
	if IsRollupFunc(funcName) {
		return getRollupArgIdxForOptimization(funcName, args...)
	}
	if IsTransformFunc(funcName) {
		return getTransformArgIdxForOptimization(funcName, args...)
	}
	if isAggrFunc(funcName) {
		return getAggrArgIdxForOptimization(funcName, args...)
	}
	return -1
}

func getAggrArgIdxForOptimization(funcName string, args ...parser.Expr) int {
	switch strings.ToLower(funcName) {
	case "bottomk", "bottomk_avg", "bottomk_max", "bottomk_median", "bottomk_last", "bottomk_min",
		"limitk", "outliers_mad", "outliersk", "quantile",
		"topk", "topk_avg", "topk_max", "topk_median", "topk_last", "topk_min":
		return 1
	case "count_values":
		return -1
	case "quantiles":
		return len(args) - 1
	default:
		return 0
	}
}

func getRollupArgIdxForOptimization(funcName string, args ...parser.Expr) int {
	// This must be kept in sync with GetRollupArgIdx()
	switch strings.ToLower(funcName) {
	case "absent_over_time":
		return -1
	case "quantile_over_time", "aggr_over_time",
		"hoeffding_bound_lower", "hoeffding_bound_upper":
		return 1
	case "quantiles_over_time":
		return len(args) - 1
	default:
		return 0
	}
}

func getTransformArgIdxForOptimization(funcName string, args ...parser.Expr) int {
	funcName = strings.ToLower(funcName)
	if isLabelManipulationFunc(funcName) {
		return -1
	}
	switch funcName {
	case "", "absent", "scalar", "union", "vector":
		return -1
	case "end", "now", "pi", "ru", "start", "step", "time":
		return -1
	case "limit_offset":
		return 2
	case "buckets_limit", "histogram_quantile", "histogram_share", "range_quantile":
		return 1
	case "histogram_quantiles":
		return len(args) - 1
	default:
		return 0
	}
}

func isLabelManipulationFunc(funcName string) bool {
	switch strings.ToLower(funcName) {
	case "alias", "drop_common_labels", "label_copy", "label_del", "label_graphite_group", "label_join", "label_keep", "label_lowercase",
		"label_map", "label_match", "label_mismatch", "label_move", "label_replace", "label_set", "label_transform",
		"label_uppercase", "label_value":
		return true
	default:
		return false
	}
}
