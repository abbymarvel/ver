package ddprofiler.analysis;

import ddprofiler.analysis.modules.EntityAnalyzer;
import ddprofiler.core.config.ProfilerConfig;

public class AnalyzerFactory {

    public static NumericalAnalysis makeNumericalAnalyzer(int pseudoRandomSeed) {
        NumericalAnalyzer na = NumericalAnalyzer.makeAnalyzer(pseudoRandomSeed);
        return na;
    }

    public static TextualAnalysis makeTextualAnalyzer(ProfilerConfig pc, int pseudoRandomSeed) {
        TextualAnalyzer ta = TextualAnalyzer.makeAnalyzer(pseudoRandomSeed, pc);
        return ta;
    }
}
