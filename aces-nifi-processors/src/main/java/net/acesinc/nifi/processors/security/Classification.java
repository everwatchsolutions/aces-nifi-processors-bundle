/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.acesinc.nifi.processors.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jeremytaylor
 */
public class Classification {

    private List<String> levels = new ArrayList<>();
    @JsonProperty(required = false)
    private List<String> compartments = new ArrayList<>();
    @JsonProperty(required = false)
    private List<String> releasabilities = new ArrayList<>();
    @JsonProperty(required = false)
    private List<String> disseminationControls = new ArrayList<>();

    /**
     * @return the levels
     */
    public List<String> getLevels() {
        return levels;
    }

    /**
     * @param levels the levels to set
     */
    public void setLevels(List<String> levels) {
        this.levels = levels;
    }

    /**
     * @return the compartments
     */
    public List<String> getCompartments() {
        return compartments;
    }

    /**
     * @param compartments the compartments to set
     */
    public void setCompartments(List<String> compartments) {
        this.compartments = compartments;
    }

    /**
     * @return the releasabilities
     */
    public List<String> getReleasabilities() {
        return releasabilities;
    }

    /**
     * @param releasabilities the releasabilities to set
     */
    public void setReleasabilities(List<String> releasabilities) {
        this.releasabilities = releasabilities;
    }

    /**
     * @return the disseminationControls
     */
    public List<String> getDisseminationControls() {
        return disseminationControls;
    }

    /**
     * @param disseminationControls the disseminationControls to set
     */
    public void setDisseminationControls(List<String> disseminationControls) {
        this.disseminationControls = disseminationControls;
    }
}
