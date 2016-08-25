/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.security;

/**
 *
 * @author jeremytaylor
 */
public class FlowAttrSecurityConfig {

    private String[] levelsToConvertTo;
    private String[] levelsCanReceive;
    private String[] abbreviatedLevelsCanReceive;
    private String[] compartments;
    private String[] disseminationControls;
    private String[] releasabilities;
    /**
     * a String delimiter
     */
    private String delim;

    /**
     * @return the levelsToConvertTo
     */
    public String[] getLevelsToConvertTo() {
        return levelsToConvertTo;
    }

    /**
     * @param levelsToConvertTo the levelsToConvertTo to set
     */
    public void setLevelsToConvertTo(String[] levelsToConvertTo) {
        this.levelsToConvertTo = levelsToConvertTo;
    }

    /**
     * @return the levelsCanReceive
     */
    public String[] getLevelsCanReceive() {
        return levelsCanReceive;
    }

    /**
     * @param levelsCanReceive the levelsCanReceive to set
     */
    public void setLevelsCanReceive(String[] levelsCanReceive) {
        this.levelsCanReceive = levelsCanReceive;
    }

    /**
     * @return the abbreviatedLevelsCanReceive
     */
    public String[] getAbbreviatedLevelsCanReceive() {
        return abbreviatedLevelsCanReceive;
    }

    /**
     * @param abbreviatedLevelsCanReceive the abbreviatedLevelsCanReceive to set
     */
    public void setAbbreviatedLevelsCanReceive(String[] abbreviatedLevelsCanReceive) {
        this.abbreviatedLevelsCanReceive = abbreviatedLevelsCanReceive;
    }

    /**
     * @return the compartments
     */
    public String[] getCompartments() {
        return compartments;
    }

    /**
     * @param compartments the compartments to set
     */
    public void setCompartments(String[] compartments) {
        this.compartments = compartments;
    }

    /**
     * @return the disseminationControls
     */
    public String[] getDisseminationControls() {
        return disseminationControls;
    }

    /**
     * @param disseminationControls the disseminationControls to set
     */
    public void setDisseminationControls(String[] disseminationControls) {
        this.disseminationControls = disseminationControls;
    }

    /**
     * @return the releasabilities
     */
    public String[] getReleasabilities() {
        return releasabilities;
    }

    /**
     * @param releasabilities the releasabilities to set
     */
    public void setReleasabilities(String[] releasabilities) {
        this.releasabilities = releasabilities;
    }

    /**
     * @return the delim
     */
    public String getDelim() {
        return delim;
    }

    /**
     * @param delim the delim to set
     */
    public void setDelim(String delim) {
        this.delim = delim;
    }

}
