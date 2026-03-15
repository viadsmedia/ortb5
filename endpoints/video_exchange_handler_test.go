package endpoints

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestVideoExchangeSyncPipelineCfgPropagatesSourceAndCampaignFields(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	campaigns := newCampaignStore("")
	handler.SetCampaignStore(campaigns)

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	campaign := campaigns.create(&Campaign{
		Name:            "Demand Campaign",
		AdvertiserID:    "adv-1",
		PublisherID:     "pub-1",
		VASTTagURL:      "https://demand.example/vast",
		OrtbEndpointURL: "https://demand.example/openrtb",
		FloorCPM:        4.25,
		BAdv:            []string{"blocked.example"},
		BCat:            []string{"IAB25"},
		MimeTypes:       []string{"video/mp4", "application/dash+xml"},
		Protocols:       []int{7, 8},
		APIs:            []int{2, 7},
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:           "placement-1",
		PublisherID:  "pub-1",
		Name:         "Supply Source",
		Environment:  VideoEnvCTV,
		Placement:    PlacementInStream,
		DomainOrApp:  "tv.example.com",
		BundleID:     "com.example.fallback",
		ContentURL:   "https://tv.example.com/watch/live",
		TargetingExt: map[string]interface{}{"channel": "sports", "premium": true},
		MinDuration:  15,
		MaxDuration:  30,
		Bidders:      []string{"appnexus", "ix"},
		FloorCPM:     2.5,
		CampaignID:   campaign.ID,
		SellerDomain: "exchange.example.com",
		Active:       true,
		TimeoutMS:    650,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if registered.DomainOrApp != "tv.example.com" {
		t.Fatalf("expected domain_or_app from source, got %q", registered.DomainOrApp)
	}
	if registered.ContentURL != "https://tv.example.com/watch/live" {
		t.Fatalf("expected content_url to sync, got %q", registered.ContentURL)
	}
	if registered.TargetingExt["channel"] != "sports" {
		t.Fatalf("expected targeting_ext.channel to sync, got %#v", registered.TargetingExt)
	}
	if registered.FloorCPM != 2.5 {
		t.Fatalf("expected source floor to remain on runtime config, got %v", registered.FloorCPM)
	}
	if registered.DemandFloorCPM != 4.25 {
		t.Fatalf("expected campaign floor to become demand floor, got %v", registered.DemandFloorCPM)
	}
	if registered.DemandVASTURL != "https://demand.example/vast" {
		t.Fatalf("expected campaign vast url, got %q", registered.DemandVASTURL)
	}
	if registered.DemandOrtbURL != "https://demand.example/openrtb" {
		t.Fatalf("expected campaign ortb url, got %q", registered.DemandOrtbURL)
	}
	if registered.SellerDomain != "exchange.example.com" {
		t.Fatalf("expected seller_domain to sync, got %q", registered.SellerDomain)
	}
	if len(registered.BAdv) != 1 || registered.BAdv[0] != "blocked.example" {
		t.Fatalf("expected badv from campaign, got %#v", registered.BAdv)
	}
	if len(registered.Protocols) != 2 || registered.Protocols[0] != 7 {
		t.Fatalf("expected campaign protocols to drive runtime config, got %#v", registered.Protocols)
	}
	if len(registered.APIs) != 2 || registered.APIs[0] != 2 {
		t.Fatalf("expected campaign apis to sync, got %#v", registered.APIs)
	}
}

func TestVideoExchangeSyncPipelineCfgFallsBackToBundleID(t *testing.T) {
	handler := NewVideoExchangeHandler("")

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:          "placement-2",
		Name:        "In-App Source",
		Environment: VideoEnvInApp,
		Placement:   PlacementRewarded,
		BundleID:    "com.example.app",
		MinDuration: 5,
		MaxDuration: 30,
		Active:      true,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if registered.DomainOrApp != "com.example.app" {
		t.Fatalf("expected bundle_id fallback, got %q", registered.DomainOrApp)
	}
}

func TestVideoExchangeSyncPipelineCfgUsesReverseSupplyLinkWhenCampaignIDMissing(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	campaigns := newCampaignStore("")
	handler.SetCampaignStore(campaigns)

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	campaign := campaigns.create(&Campaign{
		Name:            "Reverse Linked Campaign",
		AdvertiserID:    "adv-2",
		PublisherID:     "pub-2",
		OrtbEndpointURL: "https://demand.example/reverse-openrtb",
		IntegrationType: "open_rtb",
		Status:          "active",
		SupplyLinks:     []string{"placement-reverse"},
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:          "placement-reverse",
		PublisherID: "pub-2",
		Environment: VideoEnvCTV,
		Placement:   PlacementInStream,
		MinDuration: 15,
		MaxDuration: 30,
		Active:      true,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if registered.CampaignID != campaign.ID {
		t.Fatalf("expected reverse-linked campaign id %q, got %q", campaign.ID, registered.CampaignID)
	}
	if registered.DemandOrtbURL != "https://demand.example/reverse-openrtb" {
		t.Fatalf("expected reverse-linked ortb url, got %q", registered.DemandOrtbURL)
	}
}

func TestVideoExchangeSyncPipelineCfgPropagatesExtraDemandIdentity(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	campaigns := newCampaignStore("")
	handler.SetCampaignStore(campaigns)

	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	primary := campaigns.create(&Campaign{
		Name:            "Primary Campaign",
		AdvertiserID:    "adv-primary",
		PublisherID:     "pub-1",
		OrtbEndpointURL: "https://primary.example/openrtb",
		FloorCPM:        0.5,
	})
	extra := campaigns.create(&Campaign{
		Name:         "Extra Campaign",
		AdvertiserID: "adv-extra",
		PublisherID:  "pub-1",
		VASTTagURL:   "https://extra.example/vast",
		FloorCPM:     3.0,
	})

	handler.syncPipelineCfg(&VideoExchangeEntry{
		ID:          "placement-extra",
		PublisherID: "pub-1",
		Environment: VideoEnvCTV,
		Placement:   PlacementInStream,
		MinDuration: 15,
		MaxDuration: 30,
		CampaignID:  primary.ID,
		DemandLinks: []string{primary.ID, extra.ID},
		Active:      true,
	})

	if registered == nil {
		t.Fatal("expected config to be registered")
	}
	if len(registered.ExtraDemand) != 1 {
		t.Fatalf("expected 1 extra demand source, got %d", len(registered.ExtraDemand))
	}
	if registered.ExtraDemand[0].CampaignID != extra.ID {
		t.Fatalf("expected extra campaign id %q, got %q", extra.ID, registered.ExtraDemand[0].CampaignID)
	}
	if registered.ExtraDemand[0].AdvertiserID != "adv-extra" {
		t.Fatalf("expected extra advertiser id adv-extra, got %q", registered.ExtraDemand[0].AdvertiserID)
	}
	if registered.ExtraDemand[0].FloorCPM != 3.0 {
		t.Fatalf("expected extra floor 3.0, got %v", registered.ExtraDemand[0].FloorCPM)
	}
}

func TestVideoExchangeGetRecoversMissingEntryFromRuntimeConfig(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	handler.SetPipelineLookup(func(id string) *AdServerConfig {
		if id != "5679c6f87c033624" {
			return nil
		}
		return &AdServerConfig{
			PlacementID:        id,
			PublisherID:        "pub-1",
			DomainOrApp:        "tv.example.com",
			ContentURL:         "https://tv.example.com/live",
			MinDuration:        15,
			MaxDuration:        30,
			AllowedBidders:     []string{"appnexus", "ix"},
			FloorCPM:           2.1,
			CampaignID:         "camp-1",
			Active:             true,
			TimeoutMS:          700,
			VideoPlacementType: string(PlacementInStream),
			ExtraDemand:        []ExtraDemandCfg{{CampaignID: "camp-2"}},
		}
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/video/5679c6f87c033624", nil)
	handler.Get()(rec, req, httprouter.Params{{Key: "id", Value: "5679c6f87c033624"}})

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var entry VideoExchangeEntry
	if err := json.NewDecoder(rec.Body).Decode(&entry); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if entry.ID != "5679c6f87c033624" {
		t.Fatalf("expected recovered id, got %q", entry.ID)
	}
	if entry.Name != "5679c6f87c033624" {
		t.Fatalf("expected recovered name to default to placement id, got %q", entry.Name)
	}
	if entry.CampaignID != "camp-1" {
		t.Fatalf("expected campaign_id from runtime config, got %q", entry.CampaignID)
	}
	if len(entry.DemandLinks) != 2 || entry.DemandLinks[0] != "camp-1" || entry.DemandLinks[1] != "camp-2" {
		t.Fatalf("expected demand links to include runtime campaign ids, got %#v", entry.DemandLinks)
	}
	if _, ok := handler.store.get("5679c6f87c033624"); !ok {
		t.Fatal("expected missing runtime placement to be materialized into store")
	}
}

func TestVideoExchangeDeleteFallsBackToRuntimeConfig(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	handler.SetPipelineLookup(func(id string) *AdServerConfig {
		if id == "placement-runtime-only" {
			return &AdServerConfig{PlacementID: id}
		}
		return nil
	})
	var unregistered string
	handler.SetPipelineUnregister(func(id string) {
		unregistered = id
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/dashboard/video/placement-runtime-only", nil)
	handler.Delete()(rec, req, httprouter.Params{{Key: "id", Value: "placement-runtime-only"}})

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}
	if unregistered != "placement-runtime-only" {
		t.Fatalf("expected runtime placement to be unregistered, got %q", unregistered)
	}
}

func TestVideoExchangeUpdateRecoversMissingEntryFromRuntimeConfig(t *testing.T) {
	handler := NewVideoExchangeHandler("")
	handler.SetPipelineLookup(func(id string) *AdServerConfig {
		if id != "placement-update" {
			return nil
		}
		return &AdServerConfig{
			PlacementID:        id,
			PublisherID:        "pub-1",
			DomainOrApp:        "tv.example.com",
			ContentURL:         "https://tv.example.com/live",
			MinDuration:        15,
			MaxDuration:        30,
			Active:             true,
			VideoPlacementType: string(PlacementInStream),
		}
	})
	var registered *AdServerConfig
	handler.SetPipelineRegister(func(cfg *AdServerConfig) {
		registered = cfg
	})

	body := strings.NewReader(`{"name":"Recovered Placement","environment":"ctv","placement":"instream","integration_type":"open_rtb","min_duration":15,"max_duration":30,"floor_cpm":1.5,"active":true}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/dashboard/video/placement-update", body)
	handler.Update()(rec, req, httprouter.Params{{Key: "id", Value: "placement-update"}})

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	entry, ok := handler.store.get("placement-update")
	if !ok {
		t.Fatal("expected recovered entry to be present in store after update")
	}
	if entry.Name != "Recovered Placement" {
		t.Fatalf("expected updated name, got %q", entry.Name)
	}
	if registered == nil || registered.PlacementID != "placement-update" {
		t.Fatalf("expected updated placement to be synced back into runtime config, got %#v", registered)
	}
}
