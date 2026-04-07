import { useEffect } from "react";
import { useAppStore } from "../store/appStore";

export function useUrlSync() {
  const { activeTab, setActiveTab, selectedCnpj, setSelectedCnpj } =
    useAppStore();

  // Read from URL on mount
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const cnpj = params.get("cnpj");
    const tab = params.get("tab");
    if (cnpj) setSelectedCnpj(cnpj);
    if (tab) setActiveTab(tab);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Write to URL on change
  useEffect(() => {
    const params = new URLSearchParams();
    if (selectedCnpj) params.set("cnpj", selectedCnpj);
    if (activeTab) params.set("tab", activeTab);
    const search = params.toString();
    const newUrl = search ? `?${search}` : window.location.pathname;
    window.history.replaceState(null, "", newUrl);
  }, [activeTab, selectedCnpj]);
}
