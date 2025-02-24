package org.replicadb.cli;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.experimental.UtilityClass;

/**
 * Replaces the environment variables in a String.
 * The variables must be specified as follows ${varName}
 */
@UtilityClass
class EnvironmentVariableEvaluator {
    private static final String VAR_REG_EXP = "\\$\\{(.*?)\\}";
    private static final Pattern varPattern = Pattern.compile(VAR_REG_EXP);

    /**
     * Given a string with environment variables it replaces them with its value
     *
     * @param input String with references to environment variables in the form ${varName}
     * @return the string with the actual values of the environment variables.
     */
    String resolveEnvVars(String input) {

        if (input == null || input.isEmpty()) return null;

        String envVariable;
        Matcher varMatcher = varPattern.matcher(input);

        StringBuffer sb = new StringBuffer();
        while (varMatcher.find()) {
            envVariable = varMatcher.group(1);
            String replaceWith = System.getenv(envVariable);

            if (replaceWith != null) {
                varMatcher.appendReplacement(sb, replaceWith);
            }
        }
        varMatcher.appendTail(sb);
        return sb.toString();
    }

}